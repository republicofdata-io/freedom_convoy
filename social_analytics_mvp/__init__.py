from dagster import (
    file_relative_path,
    repository, 
    with_resources
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

dp_source_data_project_dir = file_relative_path(__file__, "./dp_source_data/")
dp_source_data_profile_dir = file_relative_path(__file__, "./dp_source_data/configs/")

my_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = dp_source_data_project_dir, 
        profiles_dir = dp_source_data_profile_dir, 
        key_prefix = ["dp_source_data"],
        use_build_command = True
    ),
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": dp_source_data_project_dir,
                "profiles_dir": dp_source_data_profile_dir,
            },
        ),
    },
)

@repository
def discursus_repo():
    return [
        my_assets
    ]
