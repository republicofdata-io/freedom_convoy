from dagster import (
    file_relative_path,
    repository, 
    with_resources
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

dbt_project_dir = file_relative_path(__file__, "./dp_analytics/")
dbt_profile_dir = file_relative_path(__file__, "./dp_analytics/config/")

my_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = dbt_project_dir, 
        profiles_dir = dbt_profile_dir, 
        key_prefix = ["data_analytics"],
        use_build_command = False
    ),
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": dbt_project_dir,
                "profiles_dir": dbt_profile_dir,
            },
        ),
    },
)

@repository
def discursus_repo():
    return [
        my_assets
    ]
