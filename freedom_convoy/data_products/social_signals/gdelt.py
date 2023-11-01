from dagster import (
    asset,
    AssetIn,
    AssetKey,
    AutoMaterializePolicy,
    DynamicPartitionsDefinition,
    FreshnessPolicy,
    Output
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

import pandas as pd

from freedom_convoy.utils.resources import my_resources

@asset(
    description = "Freedom convoy articles sample",
    key_prefix = ["social_signals"],
    group_name = "social_signals",
    resource_defs = {
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
)
def gdelt_articles(context):
    # Fetch articles
    query = f"""
        select * from select * from social_signals.gdelt.gdelt_gkg_sample
    """
    gdelt_articles_df = context.resources.snowflake_resource.execute_query(query)
    
    # Return asset
    return Output(
        value = gdelt_articles_df, 
        metadata = {
            "rows": gdelt_articles_df.index.size,
        },
    )