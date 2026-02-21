"""
Apptainer service management for simulation components.

This module provides utilities to start and stop Apptainer instances
for different simulation components (simulators, AVs, etc.).
"""

import socket
import subprocess
import time
from pathlib import Path
from typing import Any, Optional


def find_free_port(start_port: int = 8000, max_attempts: int = 100) -> Optional[int]:
    """
    Find a free port on the system.

    Args:
        start_port: Port number to start searching from
        max_attempts: Maximum number of ports to try

    Returns:
        A free port number, or None if no free port found
    """
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", port))
                return port
        except OSError:
            continue
    return None


class ApptainerServiceConfig:
    """Configuration for an Apptainer service."""

    def __init__(
        self,
        sif_path: str,
        startup_wait: float = 2.0,
        preferred_port: Optional[int] = None,
        extra_ports: Optional[dict[str, int]] = None,
        bind_mounts: Optional[list[tuple[str, str]]] = None,
        nv_runtime: bool = False,
    ):
        """
        Initialize service configuration.

        Args:
            sif_path: Path to the .sif container image
            startup_wait: Seconds to wait after starting the service
            preferred_port: Preferred port to start searching from (actual port will be dynamically allocated)
            extra_ports: Dict mapping env var names to preferred port starts (e.g., {"CARLA_PORT": 2000})
            bind_mounts: List of (host_path, container_path) bind mounts
            nv_runtime: Whether to enable NVIDIA runtime (adds --nv)
        """
        self.sif_path = sif_path
        self.startup_wait = startup_wait
        self.preferred_port = preferred_port or 8000
        self.extra_ports = extra_ports or {}
        self.bind_mounts = bind_mounts or []
        self.nv_runtime = nv_runtime

    @staticmethod
    def _resolve_sif_path(image_path: str) -> str:
        """Resolve SIF path from config value."""
        raw = Path(image_path)
        if raw.is_absolute() or raw.exists():
            return str(raw)

        dot_sifs = Path(".sifs") / image_path
        if dot_sifs.exists():
            return str(dot_sifs)

        sifs = Path("sifs") / image_path
        if sifs.exists():
            return str(sifs)

        return str(dot_sifs)

    @classmethod
    def from_component_spec(
        cls,
        component_spec: dict[str, Any],
    ) -> Optional["ApptainerServiceConfig"]:
        """Build service config strictly from task component spec."""
        image_path = component_spec.get("image_path")
        preferred_port = component_spec.get("preferred_port", 8000)
        extra_ports = component_spec.get("extra_ports")
        config_path = component_spec.get("config_path")
        bind_mounts = component_spec.get("bind_mounts")

        if image_path is None:
            print("Missing required field 'image_path' in component spec")
            return None

        if not isinstance(extra_ports, dict):
            extra_ports = {}

        if not isinstance(bind_mounts, list):
            bind_mounts = []

        normalized_bind_mounts = [
            (str(host), str(container)) for host, container in bind_mounts
        ]

        if config_path:
            config_host_path = Path(str(config_path)).expanduser().resolve()
            is_dir = str(config_path).endswith("/") or config_host_path.is_dir()
            if is_dir:
                config_container_path = "/mnt/config"
            else:
                config_container_path = f"/mnt/config/{config_host_path.name}"

            normalized_bind_mounts.append(
                (str(config_host_path), config_container_path)
            )

        nv_runtime = bool(component_spec.get("nv_runtime", False))

        try:
            return cls(
                sif_path=cls._resolve_sif_path(str(image_path)),
                preferred_port=int(preferred_port),
                extra_ports={
                    str(key): int(value) for key, value in extra_ports.items()
                },
                bind_mounts=normalized_bind_mounts,
                nv_runtime=nv_runtime,
            )
        except (TypeError, ValueError):
            print("Invalid component spec types for Apptainer service config")
            return None

    def get_start_command(self, instance_name: str, ports: dict[str, int]) -> list[str]:
        """
        Get the command to start this service.

        Args:
            instance_name: Name for the Apptainer instance
            ports: Dict mapping env var names to allocated ports (includes "PORT" and any extra ports)

        Returns:
            Command list for starting the service
        """
        cmd = ["apptainer", "instance", "start"]

        # Pass all allocated ports via environment variables
        for env_var, port in ports.items():
            cmd.extend(["--env", f"{env_var}={port}"])

        # Bind host folders/files into the container
        for host_path, container_path in self.bind_mounts:
            cmd.extend(["--bind", f"{host_path}:{container_path}"])

        if self.nv_runtime:
            cmd.append("--nv")

        # Add the SIF file and instance name
        cmd.extend([self.sif_path, instance_name])

        return cmd

    def get_run_command(self) -> list[str]:
        """Get the command to run this service as a one-shot Apptainer container."""
        cmd = ["apptainer", "run", "--containall"]

        for host_path, container_path in self.bind_mounts:
            cmd.extend(["--bind", f"{host_path}:{container_path}"])

        if self.nv_runtime:
            cmd.append("--nv")

        cmd.append(self.sif_path)
        return cmd

    @staticmethod
    def get_stop_command(instance_name: str) -> list[str]:
        """
        Get the command to stop this service.

        Args:
            instance_name: Name of the Apptainer instance to stop

        Returns:
            Command list for stopping the service
        """
        return ["apptainer", "instance", "stop", instance_name]


class ApptainerServiceManager:
    """Manager for starting and stopping Apptainer services."""

    def __init__(self):
        """Initialize the service manager."""
        self.running_instances: dict[str, dict[str, int]] = (
            {}
        )  # Maps service_name -> ports_dict
        self.component_to_instance: dict[str, str] = {}

    def start_component_service(
        self,
        component_spec: dict[str, Any],
        component_kind: str = "component",
    ) -> Optional[dict]:
        """Start an Apptainer service for a component from its task spec."""
        component_name = str(component_spec.get("name", "unknown"))
        return self._start_service(
            component_kind=component_kind,
            component_name=component_name,
            component_spec=component_spec,
        )

    def _start_service(
        self,
        component_kind: str,
        component_name: str,
        component_spec: dict[str, Any],
    ) -> Optional[dict]:
        """
        Internal method to start an Apptainer service.

        Args:
            component_kind: Kind of component (e.g., "simulator", "av")
            component_name: Name of the specific component

        Returns:
            Dict with 'url', 'port', and 'service_name' if service started successfully, None otherwise
        """
        print(f"Starting Apptainer service for {component_kind}: {component_name}")

        config = ApptainerServiceConfig.from_component_spec(component_spec)
        if config is None:
            print(f"Invalid task spec for {component_kind}: {component_name}")
            return None

        # Dynamically allocate the main port
        allocated_port = find_free_port(start_port=config.preferred_port)
        if allocated_port is None:
            print(f"Failed to find a free port for {component_kind}: {component_name}")
            return None

        print(f"Allocated main port: {allocated_port}")

        # Allocate extra ports if needed
        allocated_ports = {"PORT": allocated_port}
        for env_var, preferred_start in config.extra_ports.items():
            extra_port = find_free_port(start_port=preferred_start)
            if extra_port is None:
                print(f"Failed to find a free port for {env_var}")
                return None
            allocated_ports[env_var] = extra_port
            print(f"Allocated {env_var}: {extra_port}")

        service_name = f"{component_name}-{allocated_port}"

        try:
            command = config.get_start_command(service_name, allocated_ports)
            print(f"Running command: {' '.join(command)}")

            proc = subprocess.run(command, capture_output=True, text=True, timeout=10)

            if proc.returncode != 0:
                print(f"Failed to start Apptainer instance: {proc.stderr}")
                return None

            # Wait for service to start
            time.sleep(config.startup_wait)

            service_url = f"http://localhost:{allocated_port}"
            print(f"Apptainer instance '{service_name}' started successfully")
            print(f"Service URL: {service_url}")
            print(f"Service Port: {allocated_port}")
            for env_var, port in allocated_ports.items():
                if env_var != "PORT":
                    print(f"  {env_var}: {port}")

            self.running_instances[service_name] = allocated_ports
            self.component_to_instance[f"{component_kind}:{component_name}"] = (
                service_name
            )

            return {
                "url": service_url,
                "port": allocated_port,
                "ports": allocated_ports,
                "service_name": service_name,
            }
        except Exception as e:
            print(f"Failed to start Apptainer service: {e}")
            return None

    def stop_component_service(
        self, component_name: str, component_kind: str = "component"
    ):
        """Stop an Apptainer service for a specific component."""
        self._stop_service(component_kind=component_kind, component_name=component_name)

    def _stop_service(
        self,
        component_kind: str,
        component_name: str,
    ):
        """
        Internal method to stop an Apptainer service.

        Args:
            component_kind: Kind of component (e.g., "simulator", "av")
            component_name: Name of the specific component
        """
        # Find the service name for this component
        component_key = f"{component_kind}:{component_name}"
        service_name = self.component_to_instance.get(component_key)

        if service_name is None:
            print(f"No active service found for {component_kind}: {component_name}")
            return

        try:
            command = ApptainerServiceConfig.get_stop_command(service_name)
            print(f"Stopping Apptainer instance: {service_name}")
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=10,
            )

            if proc.returncode != 0:
                print(f"Failed to stop Apptainer instance: {proc.stderr}")
                return

            ports = self.running_instances.get(service_name, {})
            main_port = ports.get("PORT", "unknown")
            print(f"Apptainer instance '{service_name}' (port {main_port}) stopped")

            # Remove from running instances
            self.running_instances.pop(service_name, None)
            self.component_to_instance.pop(component_key, None)
        except Exception as e:
            print(f"Failed to stop Apptainer service: {e}")

    def stop_all_services(self):
        """Stop all active Apptainer services."""
        for service_name in list(self.running_instances.keys()):
            try:
                command = ApptainerServiceConfig.get_stop_command(service_name)
                print(f"Stopping Apptainer instance: {service_name}")
                proc = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if proc.returncode != 0:
                    print(f"Failed to stop Apptainer instance: {proc.stderr}")
                    continue

                ports = self.running_instances[service_name]
                main_port = ports.get("PORT", "unknown")
                print(f"Apptainer instance '{service_name}' (port {main_port}) stopped")
            except Exception as e:
                print(f"Failed to stop Apptainer instance {service_name}: {e}")

        self.running_instances.clear()
        self.component_to_instance.clear()
