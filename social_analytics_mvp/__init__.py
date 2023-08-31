from dagster import (
    Definitions,
    file_relative_path,
    load_assets_from_package_module,
    with_resources
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import DuckDBPandasIOManager

from social_analytics_mvp import dp_data_prep


dp_source_data_project_dir = file_relative_path(__file__, "./dp_source_data/")
dp_source_data_profile_dir = file_relative_path(__file__, "./dp_source_data/configs/")

my_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = dp_source_data_project_dir, 
        profiles_dir = dp_source_data_profile_dir, 
        key_prefix = ["dp_source_data"],
        use_build_command = True,
        io_manager_key="duckdb_io_manager"
    ) + 
    load_assets_from_package_module(dp_data_prep),
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": dp_source_data_project_dir,
                "profiles_dir": dp_source_data_profile_dir,
            },
        ),
        "duckdb_io_manager": DuckDBPandasIOManager(
            database="social_analytics_mvp.db"
        )
    },
)

defs = Definitions(
    assets=my_assets,
)