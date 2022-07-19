import argparse
import os
from typing import Dict, Optional

from dbt.adapters.factory import reset_adapters, register_adapter, get_adapter
from dbt.config import RuntimeConfig
from dbt.flags import DEFAULT_PROFILES_DIR, set_from_args

from dbt_dry_run.execution import dry_run_manifest
from dbt_dry_run.models import Manifest, Profile
from dbt_dry_run.models.profile import read_profiles
from dbt_dry_run.result_reporter import ResultReporter

parser = argparse.ArgumentParser(description="Dry run DBT")
parser.add_argument(
    "profile", metavar="PROFILE", type=str, help="The profile to dry run against"
)
parser.add_argument(
    "--manifest-path",
    default="manifest.json",
    help="The location of the compiled manifest.json",
)
parser.add_argument("--target", type=str, help="The target to dry run against")
parser.add_argument(
    "--profiles-dir",
    type=str,
    default="~/.dbt/",
    help="Override default profiles directory from ~/.dbt",
)
parser.add_argument(
    "--project-dir",
    type=str,
    default=".",
    help="Override where to search for `dbt_project.yml`"
)
parser.add_argument(
    "--ignore-result",
    action="store_true",
    help="Always exit 0 even if there are failures",
)
parser.add_argument(
    "--check-columns",
    action="store_true",
    help="Whether dry runner should check column metadata has been documented accurately"
)
parser.add_argument(
    "--model", help="Only dry run this model and its upstream dependencies"
)
parser.add_argument(
    "--verbose", action="store_true", help="Output verbose error messages"
)
parser.add_argument("--report-path", type=str, help="Json path to dump report to")

PROFILE_FILENAME = "profiles.yml"


def read_profiles_file(path: str) -> Dict[str, Profile]:
    profile_filepath = os.path.join(path, PROFILE_FILENAME)
    if not os.path.exists(profile_filepath):
        raise FileNotFoundError(
            f"Could not find '{PROFILE_FILENAME}' at '{profile_filepath}'"
        )
    with open(profile_filepath) as f:
        file_contents = f.read()
    return read_profiles(file_contents)

class PseudoArgs:
    def __init__(
        self,
        threads: Optional[int] = 1,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        project_dir: Optional[str] = None,
        vars: Optional[str] = "{}",
    ):
        self.threads = threads
        if target:
            self.target = target  # We don't want target in args context if it is None
        self.profiles_dir = profiles_dir or DEFAULT_PROFILES_DIR
        self.project_dir = project_dir
        self.vars = vars  # json.dumps str
        self.dependencies = []
        self.single_threaded = threads == 1


def run() -> int:
    parsed_args = parser.parse_args()
    manifest = Manifest.from_filepath(parsed_args.manifest_path)
    profiles = read_profiles_file(parsed_args.profiles_dir)
    try:
        profile = profiles[parsed_args.profile]
    except KeyError:
        raise KeyError(
            f"Could not find profile '{parsed_args.profile}' in profiles: {list(profiles.keys())}"
        )

    active_output = parsed_args.target or profile.target

    set_from_args(parsed_args, parsed_args)
    dbt_project, dbt_profile = RuntimeConfig.collect_parts(parsed_args)
    dbt_config = RuntimeConfig.from_parts(dbt_project, dbt_profile, parsed_args)

    reset_adapters()
    register_adapter(dbt_config)
    adapter = get_adapter(dbt_config)

    dry_run_results = dry_run_manifest(manifest, adapter, parsed_args.model)

    reporter = ResultReporter(dry_run_results, set(), parsed_args.verbose)
    exit_code = reporter.report_and_check_results()
    if parsed_args.report_path:
        reporter.write_results_artefact(parsed_args.report_path)

    if parsed_args.ignore_result:
        exit_code = 0
    return exit_code
