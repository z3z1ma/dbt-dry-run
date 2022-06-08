from typing import Dict, Optional

from dbt_dry_run.models import BigQueryFieldType, Report, Table
from dbt_dry_run.models.manifest import Manifest, ManifestColumn


def get_column_names_equal(
    manifest: Dict[str, ManifestColumn], dry_run: Table
) -> Optional[str]:
    dry_run_column_names = list(map(lambda field: field.name, dry_run.fields))
    missing_columns_in_manifest = set(dry_run_column_names) - set(manifest.keys())
    extra_columns_in_manifest = set(manifest.keys()) - set(dry_run_column_names)

    rule_failure = missing_columns_in_manifest or extra_columns_in_manifest
    if not rule_failure:
        return None

    error_message = ""
    if missing_columns_in_manifest:
        error_message += f"Missing columns in manifest: {missing_columns_in_manifest}."
    if extra_columns_in_manifest:
        error_message += f"Extra columns in manifest: {extra_columns_in_manifest}."

    return error_message


def get_boolean_column_names(
    manifest: Dict[str, ManifestColumn], dry_run: Table
) -> Optional[str]:
    failing_fields = []
    for field in dry_run.fields:
        field_is_bool = (
            field.type_ == BigQueryFieldType.BOOL
            or field.type_ == BigQueryFieldType.BOOLEAN
        )
        field_has_is_prefix = field.name.startswith("is_")
        if field_is_bool and not field_has_is_prefix:
            failing_fields.append(field)

    rule_failure = len(failing_fields)
    if not rule_failure:
        return None

    failing_field_names = list(map(lambda field: field.name, failing_fields))
    error_message = (
        f"Fields {failing_field_names} are boolean but do not begin with 'is_'"
    )

    return error_message


def lint_columns(manifest: Manifest, report: Report) -> None:
    for node in report.nodes:
        if not node.table:
            continue
        manifest_columns = manifest.nodes[node.unique_id].columns
        if not len(manifest_columns):
            # Skip undocumented models
            continue

        error_message = get_column_names_equal(manifest_columns, node.table)
        if error_message:
            print(f"ERROR: Node {node.unique_id} failed: {error_message}")

        error_message = get_boolean_column_names(manifest_columns, node.table)
        if error_message:
            print(f"ERROR: Node {node.unique_id} failed: {error_message}")
