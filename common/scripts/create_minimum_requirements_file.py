import json
import subprocess
import argparse
from typing import List, Dict


def get_installed_versions() -> Dict[str, str]:
    """Return a mapping of package name to its installed version."""
    result = subprocess.run(["pip", "list", "--format=json"], stdout=subprocess.PIPE, check=True, text=True)
    pkgs = json.loads(result.stdout)
    return {pkg["name"].lower(): pkg["version"] for pkg in pkgs}


def get_top_level_packages() -> List[str]:
    """Return a sorted list of top-level packages (not dependencies of any other package)."""
    result = subprocess.run(["pipdeptree", "--json"], stdout=subprocess.PIPE, check=True, text=True)
    tree = json.loads(result.stdout)

    all_packages = {pkg["package"]["key"] for pkg in tree}
    all_dependencies = {dep["key"] for pkg in tree for dep in pkg.get("dependencies", [])}

    return sorted(all_packages - all_dependencies)


def write_requirements(top_level: List[str], versions: Dict[str, str], with_versions: bool, output_file: str) -> None:
    """Write top-level packages to requirements file, optionally with versions."""
    with open(output_file, "w") as f:
        for pkg in top_level:
            line = f"{pkg}=={versions[pkg]}" if with_versions else pkg
            f.write(f"{line}\n")
    print(f"Wrote {len(top_level)} top-level packages to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate requirements.txt with only top-level packages.")
    parser.add_argument(
        "--with-versions", action="store_true", help="Include version numbers in the requirements file"
    )
    parser.add_argument(
        "--output", default="requirements.txt", help="Output filename (default: requirements.txt)"
    )
    args = parser.parse_args()

    top_level = get_top_level_packages()
    versions = get_installed_versions() if args.with_versions else {}
    write_requirements(top_level, versions, args.with_versions, args.output)


if __name__ == "__main__":
    main()
