import os
from dataclasses import dataclass

from dbt.adapters.factory import reset_adapters, register_adapter, get_adapter
from dbt.config import RuntimeConfig
from dbt.contracts.connection import Connection
from dbt.contracts.graph.manifest import Manifest
from dbt.flags import DEFAULT_PROFILES_DIR, set_from_args


@dataclass(frozen=True)
class DbtArgs:
    profiles_dir: str = DEFAULT_PROFILES_DIR
    project_dir: str = os.getcwd()


def set_dbt_args(args: DbtArgs):
    set_from_args(args, args)


class ProjectService:
    def __init__(self, args: DbtArgs):
        self._args = args
        set_dbt_args(self._args)
        dbt_project, dbt_profile = RuntimeConfig.collect_parts(self._args)
        self._config = RuntimeConfig.from_parts(dbt_project, dbt_profile, self._args)
        reset_adapters()
        register_adapter(self._config)
        self._adapter = get_adapter(self._config)

    def get_connection(self) -> Connection:
        return self._adapter.get_thread_connection()

    def get_dbt_manifest(self) -> Manifest:
        from dbt.parser.manifest import ManifestLoader
        return ManifestLoader.get_full_manifest(self._config)
