import os
from typing import Optional

import typer
from dbt.flags import DEFAULT_PROFILES_DIR
from typer import Argument

from dbt_dry_run.adapter.service import ProjectService, DbtArgs
from dbt_dry_run.execution import dry_run_manifest
from dbt_dry_run.result_reporter import ResultReporter

# parser = argparse.ArgumentParser(description="Dry run DBT")
# parser.add_argument(
#     "profile", metavar="PROFILE", type=str, help="The profile to dry run against"
# )
# parser.add_argument(
#     "--manifest-path",
#     default="manifest.json",
#     help="The location of the compiled manifest.json",
# )
# parser.add_argument("--target", type=str, help="The target to dry run against")
# parser.add_argument(
#     "--profiles-dir",
#     type=str,
#     default="~/.dbt/",
#     help="Override default profiles directory from ~/.dbt",
# )
# parser.add_argument(
#     "--project-dir",
#     type=str,
#     default=".",
#     help="Override where to search for `dbt_project.yml`"
# )
# parser.add_argument(
#     "--ignore-result",
#     action="store_true",
#     help="Always exit 0 even if there are failures",
# )
# parser.add_argument(
#     "--check-columns",
#     action="store_true",
#     help="Whether dry runner should check column metadata has been documented accurately"
# )
# parser.add_argument(
#     "--model", help="Only dry run this model and its upstream dependencies"
# )
# parser.add_argument(
#     "--verbose", action="store_true", help="Output verbose error messages"
# )
# parser.add_argument("--report-path", type=str, help="Json path to dump report to")

app = typer.Typer()


def dry_run(project_dir: str, profiles_dir: str, verbose: bool = False, report_path: Optional[str] = None) -> int:
    args = DbtArgs(project_dir=project_dir, profiles_dir=profiles_dir)
    project = ProjectService(args)
    dry_run_results = dry_run_manifest(project)
    reporter = ResultReporter(dry_run_results, set(), verbose)
    exit_code = reporter.report_and_check_results()
    if report_path:
        reporter.write_results_artefact(report_path)
    return exit_code


@app.command()
def run(profiles_dir: str = Argument(DEFAULT_PROFILES_DIR, help="Where to search for `profiles.yml`"),
        project_dir: str = Argument(os.getcwd(), help="Where to search for `dbt_project.yml`"),
        verbose: bool = Argument(False, help="Output verbose error messages"),
        report_path: Optional[str] = Argument(None, help="Json path to dump report to")):
    exit_code = dry_run(project_dir, profiles_dir, verbose, report_path)
    if exit_code > 0:
        raise typer.Exit(exit_code)


if __name__ == "__main__":
    app()
