"""Keep the dynamic Python source version authoritative for CI and image tags."""

import ast
import os
import re
from pathlib import Path


def source_version(root):
    source = root / "src/qbittorrent_smart_queues/__init__.py"
    versions = [
        item.value.value
        for item in ast.parse(source.read_text()).body
        if isinstance(item, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in item.targets
        )
        and isinstance(item.value, ast.Constant)
        and isinstance(item.value.value, str)
    ]
    if len(versions) != 1 or not re.fullmatch(
        r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", versions[0]
    ):
        raise ValueError("Expected one semantic application source version")
    return versions[0]


def check_version(version, environment):
    supplied = environment.get("RELEASE_VERSION", "")
    if supplied and supplied.removeprefix("v") != version:
        raise ValueError("Requested image version differs from application source")
    if (
        environment.get("GITHUB_REF_TYPE") == "tag"
        and environment.get("GITHUB_REF_NAME", "").removeprefix("v") != version
    ):
        raise ValueError("Release tag differs from application source")
    return version


def main():
    version = check_version(
        source_version(Path(__file__).resolve().parents[1]), os.environ
    )
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a") as stream:
            stream.write(f"version={version}\n")
    print(version)


if __name__ == "__main__":
    main()
