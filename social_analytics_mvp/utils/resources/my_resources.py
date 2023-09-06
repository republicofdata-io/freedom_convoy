from dagster import config_from_pkg_resources, file_relative_path

from social_analytics_mvp.utils.resources import openai_resource, web_scraper_resource


# Configs
openai_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('social_analytics_mvp.utils.configs', 'openai_configs.yaml')
    ],
)


# Initiate resources
my_openai_resource = openai_resource.initiate_openai_resource.configured(openai_configs)
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)