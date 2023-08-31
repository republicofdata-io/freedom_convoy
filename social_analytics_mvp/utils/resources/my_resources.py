from dagster import config_from_pkg_resources, file_relative_path

from social_analytics_mvp.utils.resources import web_scraper_resource


# Initiate resources
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)