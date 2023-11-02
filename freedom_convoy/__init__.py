from dagster import (
    Definitions,
    file_relative_path,
    load_assets_from_package_module,
    with_resources
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from freedom_convoy.data_products import social_signals

social_signals_project_dir = file_relative_path(__file__, "./data_products/data_warehouse/")
social_signals_profile_dir = file_relative_path(__file__, "./data_products/data_warehouse/configs/")

my_assets = with_resources(
    load_assets_from_package_module(social_signals) +
    load_assets_from_dbt_project(
        project_dir = social_signals_project_dir, 
        profiles_dir = social_signals_profile_dir, 
        key_prefix = ["data_warehouse"],
        use_build_command = False,
    ),
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": social_signals_project_dir,
                "profiles_dir": social_signals_profile_dir,
            },
        ),
    },
)

defs = Definitions(
    assets=my_assets,
)