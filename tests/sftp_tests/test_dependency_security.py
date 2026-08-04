"""Regression tests for WP-33597.

CVE-2026-44405 (GHSA-r374-rxx8-8654) is only remediated in paramiko 5.0.0 and
later. paramiko 3.4.1 does NOT contain the fix commit (it first ships in 5.0.0),
so the dependency manifests and lockfiles must pin a paramiko version at or above
the fixed floor. These tests parse the repo's own release metadata and fail if
paramiko is pinned below 5.0.0 anywhere the tap-sftp package controls it.
"""
import os
import re

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - fallback for older interpreters
    import tomli as tomllib  # type: ignore

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# First paramiko release that contains the CVE-2026-44405 fix.
PARAMIKO_CVE_FIXED_FLOOR = (5, 0, 0)


def _version_tuple(version_str):
    parts = re.findall(r"\d+", version_str)[:3]
    return tuple(int(p) for p in parts)


def test_pyproject_paramiko_constraint_requires_cve_fixed_floor():
    with open(os.path.join(REPO_ROOT, "pyproject.toml"), "rb") as fh:
        pyproject = tomllib.load(fh)

    constraint = pyproject["tool"]["poetry"]["dependencies"]["paramiko"]
    # Extract every numeric version referenced in the constraint string and take
    # the lower bound; it must be >= the CVE-fixed floor.
    lower_bounds = re.findall(r"[><=~^]*\s*(\d+\.\d+\.\d+)", constraint)
    assert lower_bounds, f"could not parse paramiko constraint: {constraint!r}"
    assert _version_tuple(lower_bounds[0]) >= PARAMIKO_CVE_FIXED_FLOOR, (
        f"paramiko constraint {constraint!r} allows a version below the "
        f"CVE-2026-44405 fixed floor {PARAMIKO_CVE_FIXED_FLOOR}"
    )


def test_poetry_lock_pins_paramiko_at_cve_fixed_floor():
    with open(os.path.join(REPO_ROOT, "poetry.lock"), "rb") as fh:
        lock = tomllib.load(fh)

    paramiko_entries = [p for p in lock["package"] if p["name"] == "paramiko"]
    assert paramiko_entries, "paramiko package missing from poetry.lock"
    for entry in paramiko_entries:
        assert _version_tuple(entry["version"]) >= PARAMIKO_CVE_FIXED_FLOOR, (
            f"poetry.lock pins paramiko {entry['version']}, below the "
            f"CVE-2026-44405 fixed floor {PARAMIKO_CVE_FIXED_FLOOR}"
        )


def test_pipfile_lock_pins_paramiko_at_cve_fixed_floor():
    import json

    with open(os.path.join(REPO_ROOT, "Pipfile.lock"), "r", encoding="utf-8") as fh:
        pipfile_lock = json.load(fh)

    paramiko = pipfile_lock["default"]["paramiko"]
    version = paramiko["version"].lstrip("=")
    assert _version_tuple(version) >= PARAMIKO_CVE_FIXED_FLOOR, (
        f"Pipfile.lock pins paramiko {paramiko['version']}, below the "
        f"CVE-2026-44405 fixed floor {PARAMIKO_CVE_FIXED_FLOOR}"
    )
