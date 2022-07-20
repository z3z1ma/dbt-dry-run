from typing import Optional, Tuple

from google.cloud.bigquery import (
    DatasetReference,
    QueryJob,
    QueryJobConfig,
    TableReference, Client,
)
from google.cloud.exceptions import BadRequest, Forbidden, NotFound
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.models import Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.sql_runner import SQLRunner

MAX_ATTEMPT_NUMBER = 5
QUERY_TIMED_OUT = "Dry run query timed out"


class BigQuerySQLRunner(SQLRunner):
    JOB_CONFIG = QueryJobConfig(dry_run=True, use_query_cache=False)

    def __init__(self, project: ProjectService):
        self._project = project

    def node_exists(self, node: Node) -> bool:
        return self.get_node_schema(node) is not None

    def get_node_schema(self, node: Node) -> Optional[Table]:
        try:
            dataset = DatasetReference(node.database, node.db_schema)
            table_ref = TableReference(dataset, node.alias)
            bigquery_table = self.client.get_table(table_ref)

            return Table.from_bigquery_table(bigquery_table)
        except NotFound:
            return None

    @property
    def client(self) -> Client:
        return self._project.get_connection().handle

    def close(self) -> None:
        pass

    @retry(
        retry=retry_if_exception_type(BadRequest),
        stop=stop_after_attempt(MAX_ATTEMPT_NUMBER),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
    )
    def query(
            self, sql: str
    ) -> Tuple[DryRunStatus, Optional[Table], Optional[Exception]]:
        exception = None
        table = None
        try:
            query_job = self.client.query(sql, job_config=self.JOB_CONFIG)
            table = self.get_schema_from_query_job(query_job)
            status = DryRunStatus.SUCCESS
        except (Forbidden, BadRequest, NotFound) as e:
            status = DryRunStatus.FAILURE
            if QUERY_TIMED_OUT in str(e):
                raise
            exception = e
        return status, table, exception

    @staticmethod
    def get_schema_from_query_job(query_job: QueryJob) -> Table:
        job_fields_raw = query_job._properties["statistics"]["query"]["schema"][
            "fields"
        ]
        job_fields = [TableField.parse_obj(field) for field in job_fields_raw]
        return Table(fields=job_fields)
