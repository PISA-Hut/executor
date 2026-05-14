import argparse
import json
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import dotenv
from loguru import logger

from simcore.engine import SimulationEngine

from executor.apptainer_utils.apptainer_manager import ApptainerServiceManager
from executor.docker_utils.docker_manager import DockerServiceManager
from executor.log_capture import LogCapture, install as install_log_capture
from executor.log_streamer import LogStreamer
from executor.manager_client import ManagerClient
from executor.service_manager import ServiceManager
from executor.staging import stage_task_inputs
from executor.system import collect_executor_identity
from executor.utils import (
    build_runner_spec,
    build_services_spec,
    sanitize_path,
)

dotenv.load_dotenv()


@dataclass
class RunContext:
    """Live state the SIGTERM/SIGINT handler may need to flush at any
    point in the run. Optional fields stay None until the corresponding
    resource has been wired up so the handler can skip cleanly."""

    client: ManagerClient
    capture: LogCapture
    task_id: int | None = None
    log_streamer: LogStreamer | None = None
    service_manager: ServiceManager | None = None
    engine: SimulationEngine | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _useful_count(engine: SimulationEngine | None) -> int:
    return int(getattr(engine, "completed_concrete_runs", 0)) if engine is not None else 0


def _setup_logging(level: str) -> None:
    logger.remove()
    logger.add(
        sink=sys.stdout,
        level=level.upper(),
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )


def _install_shutdown_handler(ctx: RunContext) -> None:
    """Report task aborted/failed + flush logs + stop containers on
    SIGTERM/SIGINT. SLURM sends SIGTERM ~60 s before SIGKILL on time
    limit (see scripts/run.sh `--signal=TERM@60`). `ctx` mutates as
    the run progresses; the handler reads whatever's set at signal
    time and skips unset resources."""

    def handler(signum: int, _frame) -> None:
        # SIGINT = user. SIGTERM = scancel OR pre-kill; distinguish by
        # comparing SLURM_JOB_END_TIME with now.
        is_abort = signum == signal.SIGINT
        if signum == signal.SIGTERM:
            end_raw = os.environ.get("SLURM_JOB_END_TIME")
            if end_raw:
                try:
                    remaining = int(end_raw) - int(time.time())
                    is_abort = remaining > 90
                except ValueError:
                    pass
            else:
                is_abort = True

        logger.warning(
            f"Received signal {signum}; reporting task "
            f"{'aborted' if is_abort else 'failed'} and exiting"
        )
        if ctx.log_streamer is not None:
            try:
                ctx.log_streamer.stop()
            except Exception as exc:
                logger.error(f"log streamer stop failed during signal handling: {exc}")
        if ctx.task_id is not None:
            useful = _useful_count(ctx.engine)
            try:
                snap = ctx.capture.snapshot()
                if is_abort:
                    ctx.client.task_aborted(
                        ctx.task_id,
                        reason=f"Executor received signal {signum} (cancelled)",
                        log=snap,
                        concrete_scenarios_executed=useful,
                    )
                else:
                    ctx.client.task_failed(
                        ctx.task_id,
                        reason=f"Executor received signal {signum} (SLURM time limit)",
                        log=snap,
                        concrete_scenarios_executed=useful,
                    )
            except Exception as exc:
                logger.error(f"lifecycle call during signal handling: {exc}")
        if ctx.service_manager is not None:
            try:
                ctx.service_manager.stop_all_services()
            except Exception as exc:
                logger.error(f"service_manager stop during signal handling: {exc}")
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def _create_service_manager(backend: str, job_id: int) -> ServiceManager:
    service_manager_id = f"job{job_id:02d}"
    if backend == "apptainer":
        return ApptainerServiceManager(id=service_manager_id)
    if backend == "docker":
        return DockerServiceManager(id=service_manager_id)

    raise ValueError(f"Unsupported backend: {backend}")


def _execute_runner_task(ctx: RunContext, runner_spec: dict[str, Any]) -> None:
    """Drive one task's SimulationEngine and report the terminal state.
    Clean return -> succeeded; raise -> failed; SIGTERM/SIGINT -> aborted
    (handled in the signal handler). `concrete_scenarios_executed` is
    orthogonal — useful-work counter for the manager's useless-streak rule."""

    task_id = ctx.task_id
    assert task_id is not None  # main() guarantees this before calling

    try:
        ctx.engine = SimulationEngine(runner_spec)
        ctx.engine.exec()
    except KeyboardInterrupt:
        logger.warning("Task execution interrupted by user.")
        ctx.client.task_failed(
            task_id,
            reason="Task interrupted by user",
            log=ctx.capture.snapshot(),
            concrete_scenarios_executed=_useful_count(ctx.engine),
        )
    except Exception as exc:
        err_msg = str(exc) if isinstance(exc, RuntimeError) else f"{type(exc).__name__}: {exc}"
        logger.error(f"Task execution failed: {err_msg}")
        ctx.client.task_failed(
            task_id,
            reason=err_msg,
            log=ctx.capture.snapshot(),
            concrete_scenarios_executed=_useful_count(ctx.engine),
        )
    else:
        logger.info(f"Task execution succeeded for task ID: {task_id}")
        ctx.client.task_succeeded(
            task_id,
            log=ctx.capture.snapshot(),
            concrete_scenarios_executed=_useful_count(ctx.engine),
        )


def parse_args(
    maps: dict[str, int],
    avs: dict[str, int],
    simulators: dict[str, int],
    samplers: dict[str, int],
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executor process that claims and executes tasks from the manager."
    )
    parser.add_argument(
        "--av",
        type=str,
        choices=list(avs.keys()),
        default=None,
        help="Name of the AV to filter tasks by (optional)",
    )
    parser.add_argument(
        "--simulator",
        type=str,
        choices=list(simulators.keys()),
        default=None,
        help="Name of the simulator to filter tasks by (optional)",
    )
    parser.add_argument(
        "--map",
        type=str,
        choices=list(maps.keys()),
        default=None,
        help="Name of the map to filter tasks by (optional)",
    )
    parser.add_argument(
        "--task-id",
        type=int,
        default=None,
        help="Claim a specific task by ID",
    )
    parser.add_argument(
        "--scenario-id",
        type=int,
        default=None,
        help="ID of the scenario to filter tasks by (optional)",
    )
    parser.add_argument(
        "--sampler",
        type=str,
        choices=list(samplers.keys()),
        default=None,
        help="Name of the sampler to filter tasks by (optional)",
    )
    parser.add_argument(
        "--log-level",
        type=str.lower,
        choices=[
            "debug",
            "info",
            "warning",
            "error",
            "critical",
        ],
        default="INFO",
        help="Logging level for the executor (default: INFO)",
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=["apptainer", "docker"],
        default="apptainer",
        help="Container backend to use for services (default: apptainer)",
    )
    return parser.parse_args()


def main():
    client = ManagerClient()
    client.fetch()  # Fetch AVs, simulators, and samplers to cache their IDs

    args = parse_args(client.maps, client.avs, client.simulators, client.samplers)
    _setup_logging(args.log_level)

    # Capture everything the executor + simcore prints so we can PUT it back
    # to the task_run row and render it in the web UI.
    capture = LogCapture()
    install_log_capture(capture)

    # Hook SIGTERM/SIGINT early so even a pre-claim kill doesn't leave a
    # half-written row behind.
    ctx = RunContext(client=client, capture=capture)
    _install_shutdown_handler(ctx)

    logger.debug("Starting executor...")
    logger.info(f"Arguments: {args}")

    executor_info = collect_executor_identity()

    job_id = int(executor_info.get("job_id", "unknown"))

    claimed_spec = client.claim_task_spec(
        executor_info,
        task_id=args.task_id,
        av_name=args.av,
        simulator_name=args.simulator,
        map_name=args.map,
        scenario_id=args.scenario_id,
        sampler_name=args.sampler,
    )

    if claimed_spec is None:
        logger.info("No task claimed. Executor will exit.")
        return

    task_id = claimed_spec.get("task", {}).get("id")
    task_run_id = claimed_spec.get("task_run_id")
    if task_id is None:
        logger.error("Claimed spec does not contain a valid task ID. Aborting.")
        return
    logger.info(f"Claimed task with ID: {task_id} (task_run #{task_run_id})")
    ctx.task_id = task_id

    if task_run_id is not None:
        ctx.log_streamer = LogStreamer(
            capture=capture,
            manager_url=client.manager_url,
            task_run_id=int(task_run_id),
        )
        ctx.log_streamer.start()

    claimed_av = dict(claimed_spec.get("av", {}))
    claimed_simulator = dict(claimed_spec.get("simulator", {}))
    claimed_map = dict(claimed_spec.get("map", {}))
    claimed_scenario = dict(claimed_spec.get("scenario", {}))
    scenario_title = claimed_scenario.get("title", "unknown_scenario")
    logger.info(f"Claimed scenario: {scenario_title}")

    av = claimed_av.get("name", "unknown_av")
    sim = claimed_simulator.get("name", "unknown_simulator")
    map_name = claimed_map.get("name", "unknown_map")
    cla = f"{av}_{sim}"

    output_dir = str(
        f"./outputs/{cla}/{task_id}-{sanitize_path(map_name)}-{sanitize_path(scenario_title)}"
    )
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "claimed_spec.json"), "w") as f:
        json.dump(claimed_spec, f, indent=4)

    staged_root = Path(output_dir) / ".staged"
    # Monitor is required by the manager (m20260513 migration); claim
    # responses always include it. KeyError here means the manager
    # is older than this executor — surface as a configuration error.
    claimed_monitor = claimed_spec["monitor"]
    staged = stage_task_inputs(
        manager_url=client.manager_url,
        stage_root=staged_root,
        map_id=int(claimed_map["id"]),
        scenario_id=int(claimed_scenario["id"]),
        av_id=int(claimed_av["id"]),
        simulator_id=int(claimed_simulator["id"]),
        sampler_id=int(claimed_spec.get("sampler", {}).get("id", 0)),
        monitor_id=int(claimed_monitor["id"]),
    )
    logger.debug(f"Staged inputs under {staged_root}")

    services_spec = build_services_spec(
        claimed_av=claimed_av,
        claimed_simulator=claimed_simulator,
        claimed_map=claimed_map,
        claimed_scenario=claimed_scenario,
        staged=staged,
    )

    ctx.service_manager = _create_service_manager(args.backend, job_id)
    try:
        started_specs = ctx.service_manager.start(
            services_spec=services_spec,
            output_dir=output_dir,
        )

        runner_spec = build_runner_spec(
            claimed_spec=claimed_spec,
            claimed_simulator=claimed_simulator,
            claimed_av=claimed_av,
            claimed_map=claimed_map,
            claimed_scenario=claimed_scenario,
            started_specs=started_specs,
            staged=staged,
            job_id=job_id,
            output_dir=output_dir,
        )
        with open(os.path.join(output_dir, "runner_spec.json"), "w") as f:
            json.dump(runner_spec, f, indent=4)
        logger.debug(f"Runner spec available at: {os.path.join(output_dir, 'runner_spec.json')}")

        _execute_runner_task(ctx, runner_spec)
    except Exception as exc:
        logger.error(f"Executor failed with error: {exc}")
        client.task_failed(
            task_id,
            reason=f"{type(exc).__name__}: {exc}",
            log=capture.snapshot(),
            concrete_scenarios_executed=_useful_count(ctx.engine),
        )

    finally:
        if ctx.log_streamer is not None:
            ctx.log_streamer.stop()
        ctx.service_manager.stop_all_services()
        # Reclaim disk: the staged map / scenario / config bytes can be
        # tens of MB per task and the host accumulates one .staged tree
        # per (av, sim, task, map, scenario) combo. The manager keeps
        # the canonical copy, so a re-run just re-stages from scratch.
        # Errors here are non-fatal — log + carry on so a stuck
        # filesystem can't prevent the executor from exiting cleanly.
        if staged_root.exists():
            try:
                shutil.rmtree(staged_root)
                logger.debug(f"Cleaned staged inputs at {staged_root}")
            except Exception as e:
                logger.warning(f"Failed to clean staged inputs at {staged_root}: {e}")

    logger.debug("Executor finished execution.")


if __name__ == "__main__":
    main()
