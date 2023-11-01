from dagster_snowflake import snowflake_resource
from freedom_convoy.utils.resources import web_scraper_resource


# Configuration files
snowflake_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('discursus_data_platform.utils.configs', 'snowflake_configs.yaml')
    ],
)
# Initiate resources
my_snowflake_resource = snowflake_resource.configured(snowflake_configs)
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)