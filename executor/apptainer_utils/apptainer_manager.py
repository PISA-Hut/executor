import subprocess
import threading
from typing import Any, Optional

from loguru import logger

from executor.apptainer_utils.apptainer_config import ApptainerServiceConfig
from executor.service_manager import ServiceManager


class ApptainerServiceManager(ServiceManager):
    """Start/stop Apptainer services for simulator and AV.

    Uses foreground `apptainer run` so the container is a child of
    this executor process. SLURM owns the executor's process tree,
    so it can account resources against the job and propagate
    SIGTERM on time-limit / scancel without leaving orphan
    containers — unlike `apptainer instance start`, which daemonises
    and escapes that hierarchy.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Track the live subprocess for each service so we can stop
        # it later. Reader threads drain the merged stdout/stderr
        # pipe so the kernel buffer can't fill and stall the
        # container; they exit naturally when the pipe hits EOF on
        # process death.
        self._processes: dict[str, subprocess.Popen[str]] = {}
        self._readers: dict[str, threading.Thread] = {}

    def _start_backend_service(
        self,
        component_kind: str,
        component_name: str,
        component_spec: dict[str, Any],
        runtime_envs: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        config = ApptainerServiceConfig.from_component_spec(component_spec)
        if config is None:
            logger.error(f"Invalid task spec for {component_kind}: {component_name}")
            return None

        start_envs: dict[str, Any] = dict(config.extra_envs)
        start_envs.update(runtime_envs)

        allocated_port = int(runtime_envs["PORT"])
        service_name = f"{component_name}-{self.id}-{allocated_port}"

        try:
            command = config.get_run_command(start_envs)
            logger.debug(f"Spawning: {' '.join(command)}")
            # start_new_session=False (default) keeps the child in
            # the executor's process group so SIGTERM from SLURM
            # cascades. Merge stderr→stdout so a single reader
            # thread can prefix every line with the service name.
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            self._processes[service_name] = proc
            reader = threading.Thread(
                target=self._stream_output,
                args=(service_name, proc),
                daemon=True,
            )
            reader.start()
            self._readers[service_name] = reader

            if not self._wait_for_service_start(allocated_port):
                rc = proc.poll()
                if rc is not None:
                    logger.error(
                        f"Container exited before service was ready "
                        f"(rc={rc}): {service_name}"
                    )
                else:
                    logger.error(
                        f"Service did not become ready in time: {service_name}"
                    )
                self._stop_backend_service(service_name)
                return None

            service_url = f"localhost:{allocated_port}"
            logger.info(f"{component_name} service available at: {service_url}")

            self._register_started_service(
                component_kind=component_kind,
                component_name=component_name,
                service_name=service_name,
                runtime_envs=runtime_envs,
            )

            return {
                "url": service_url,
                "service_name": service_name,
            }
        except Exception as exc:
            logger.exception(f"Failed to start Apptainer service: {exc}")
            self._stop_backend_service(service_name)
            return None

    def _stop_backend_service(self, service_name: str) -> None:
        proc = self._processes.pop(service_name, None)
        reader = self._readers.pop(service_name, None)
        if proc is None:
            return

        logger.info(f"Stopping Apptainer container: {service_name} (pid {proc.pid})")
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"Container {service_name} did not exit on SIGTERM; killing"
                )
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.error(
                        f"Container {service_name} survived SIGKILL; leaking"
                    )

        if reader is not None:
            reader.join(timeout=5)

    @staticmethod
    def _stream_output(service_name: str, proc: subprocess.Popen[str]) -> None:
        stdout = proc.stdout
        if stdout is None:
            return
        try:
            for line in stdout:
                logger.info(f"[{service_name}] {line.rstrip()}")
        except Exception as exc:
            logger.warning(f"Output reader for {service_name} stopped: {exc}")
