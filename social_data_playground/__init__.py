from dagster import (
    Definitions,
    file_relative_path,
    load_assets_from_package_module,
    with_resources
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_aws.s3 import s3_pickle_io_manager, S3Resource

from social_data_playground import (
    s3_sources,
    enhance_articles
)


social_data_playground_db = file_relative_path(__file__, "./social_data_playground.db")
stage_sources_project_dir = file_relative_path(__file__, "./stage_sources/")
stage_sources_profile_dir = file_relative_path(__file__, "./stage_sources/configs/")


my_assets = with_resources(
    load_assets_from_package_module(s3_sources) + 
    load_assets_from_dbt_project(
        project_dir = stage_sources_project_dir, 
        profiles_dir = stage_sources_profile_dir, 
        key_prefix = ["stage_sources"],
        use_build_command = False,
        io_manager_key="duckdb_io_manager"
    ) + 
    load_assets_from_package_module(enhance_articles),
    resource_defs = {
        "io_manager": s3_pickle_io_manager.configured(
            {"s3_bucket": "social-data-playground", "s3_prefix": "platform"}
        ),
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": stage_sources_project_dir,
                "profiles_dir": stage_sources_profile_dir,
            },
        ),
        "duckdb_io_manager": DuckDBPandasIOManager(
            database=social_data_playground_db
        ),
        "s3": S3Resource(region_name='us-east-1')
    },
)

defs = Definitions(
    assets=my_assets,
)