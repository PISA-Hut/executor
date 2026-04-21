"""Fetch per-task inputs (map/scenario/config bytes) from the manager and lay
them out on the local filesystem so the container bind-mounts don't need a
shared FS with the manager."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from loguru import logger


@dataclass
class StagedPaths:
    """Absolute host paths created during staging."""

    xodr_dir: Path
    osm_dir: Path
    scenario_dir: Path
    av_config: Path
    simulator_config: Path
    sampler_config: Optional[Path]


def _fetch_into(session: requests.Session, url: str, dest: Path, timeout: int) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Fetching {url} -> {dest}")
    with session.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    fh.write(chunk)


def stage_task_inputs(
    manager_url: str,
    stage_root: Path,
    map_id: int,
    scenario_id: int,
    av_id: int,
    simulator_id: int,
    sampler_id: int,
    timeout: int = 60,
) -> StagedPaths:
    """Download everything the task needs into `stage_root`.

    Layout produced:
        <stage_root>/map/xodr/<rel files under map_file relative_path "xodr/...">
        <stage_root>/map/osm/<rel files under map_file relative_path "osm/...">
        <stage_root>/scenario/<scenario_file relative_path...>
        <stage_root>/config/{av,simulator,sampler}.yaml

    The wrapper container bind-mounts `map/xodr` -> `/mnt/map/xodr`,
    `map/osm` -> `/mnt/map/osm`, and `scenario` -> `/mnt/scenario`, so the
    relative paths saved in the database must mirror that layout (e.g. the
    map row for `tyms` stores its files as `xodr/tyms.xodr` and
    `osm/tyms.osm`).
    """
    if stage_root.exists():
        shutil.rmtree(stage_root)
    stage_root.mkdir(parents=True)

    map_dir = stage_root / "map"
    xodr_dir = map_dir / "xodr"
    osm_dir = map_dir / "osm"
    scenario_dir = stage_root / "scenario"
    config_dir = stage_root / "config"
    for d in (xodr_dir, osm_dir, scenario_dir, config_dir):
        d.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    map_listing = session.get(
        f"{manager_url}/map/{map_id}/file", timeout=timeout
    )
    map_listing.raise_for_status()
    for entry in map_listing.json():
        rel = entry["relative_path"]
        dest = map_dir / rel
        _fetch_into(
            session, f"{manager_url}/map/{map_id}/file/{rel}", dest, timeout
        )

    scn_listing = session.get(
        f"{manager_url}/scenario/{scenario_id}/file", timeout=timeout
    )
    scn_listing.raise_for_status()
    for entry in scn_listing.json():
        rel = entry["relative_path"]
        dest = scenario_dir / rel
        _fetch_into(
            session,
            f"{manager_url}/scenario/{scenario_id}/file/{rel}",
            dest,
            timeout,
        )

    av_config = config_dir / "av.yaml"
    _fetch_into(session, f"{manager_url}/av/{av_id}/config", av_config, timeout)

    sim_config = config_dir / "simulator.yaml"
    _fetch_into(
        session,
        f"{manager_url}/simulator/{simulator_id}/config",
        sim_config,
        timeout,
    )

    sampler_config: Optional[Path] = None
    try:
        sp = config_dir / "sampler.yaml"
        _fetch_into(
            session,
            f"{manager_url}/sampler/{sampler_id}/config",
            sp,
            timeout,
        )
        sampler_config = sp
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 404:
            logger.debug(f"Sampler {sampler_id} has no config; skipping")
        else:
            raise

    return StagedPaths(
        xodr_dir=xodr_dir.resolve(),
        osm_dir=osm_dir.resolve(),
        scenario_dir=scenario_dir.resolve(),
        av_config=av_config.resolve(),
        simulator_config=sim_config.resolve(),
        sampler_config=sampler_config.resolve() if sampler_config else None,
    )
