import dotenv
import json
from pprint import pprint
from typing import Any
from pathlib import Path

from worker.manager_client import ManagerClient
from worker.system import collect_worker_identity
from worker.apptainer_service import ApptainerServiceManager

from worker.runner.runner import Runner

dotenv.load_dotenv()


def main():
    print("Starting worker...")
    client = ManagerClient()
    slurm_info = collect_worker_identity()
    worker_info = client.register_worker(slurm_info)
    print(f"Registered worker with ID: {worker_info['id']}")
    job_id = slurm_info.get("job_id", "unknown")

    assert isinstance(worker_info["id"], int)
    response = client.claim_task(worker_info["id"])
    if response is None:
        print("No tasks available to claim.")
        return
    assert isinstance(response, dict)

    spec: dict[str, dict[str, Any]] = response
    task_id = spec["task"].pop("id", None)
    print(f"Claimed task ID: {task_id}")

    pprint(spec)
    print()

    spec["task"]["output_dir"] = f"./output"
    spec["task"]["job_id"] = str(job_id)
    spec["runtime"] = {"dt": 0.01}
    assert isinstance(spec["scenario"], dict)

    pprint(spec)
    print()
    print(f"Claimed task: {task_id}")

    # Initialize the Apptainer service manager
    service_manager = ApptainerServiceManager()

    # Get component configs and start appropriate Apptainer services
    simulator_spec = dict(spec.get("simulator", {}))
    av_spec = dict(spec.get("av", {}))

    map_spec = spec.get("map", {})
    scenario_spec = spec.get("scenario", {})
    task_spec = spec.get("task", {})

    osm_path = str(Path(map_spec.get("osm_path", "./maps/osm")).resolve())
    xodr_path = str(Path(map_spec.get("xodr_path", "./maps/xodr")).resolve())
    scenario_path = str(
        Path(scenario_spec.get("scenario_path", "./scenarios")).resolve()
    )
    output_path = str(Path(task_spec.get("output_dir", "./output")))
    Path(output_path).mkdir(parents=True, exist_ok=True)

    bind_mounts: list[tuple[str, str]] = [
        (xodr_path, "/mnt/map/xodr"),
        (osm_path, "/mnt/map/osm"),
        (scenario_path, "/mnt/scenario"),
        (output_path, "/mnt/output"),
    ]

    simulator_spec["bind_mounts"] = bind_mounts
    av_spec["bind_mounts"] = bind_mounts

    for component_kind, component_spec in (
        ("simulator", simulator_spec),
        ("av", av_spec),
    ):
        component_name = component_spec.get("name", "unknown")
        print(f"{component_kind.title()}: {component_name}")

        service_info = service_manager.start_component_service(
            component_spec=component_spec,
            component_kind=component_kind,
        )
        if service_info:
            spec[component_kind]["service_info"] = service_info
            print(
                f"{component_kind.title()} service available at: {service_info['url']}"
            )

    try:
        runner = Runner(spec)
        runner.exec()
    finally:
        service_manager.stop_all_services()


if __name__ == "__main__":
    main()
