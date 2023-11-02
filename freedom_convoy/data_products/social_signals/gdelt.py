from dagster import (
    asset,
    Output
)

import pandas as pd

from freedom_convoy.utils.resources import my_resources


@asset(
    description = "Freedom convoy articles sample",
    key_prefix = ["social_signals"],
    group_name = "social_signals",
    resource_defs = {
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
    compute_kind = "snowflake",
)
def media_sources(context):
    # Fetch media sources
    query = f"""
        select * from social_signals.gdelt.media_sources
    """
    media_sources_df = context.resources.snowflake_resource.execute_query(query, fetch_results=True, use_pandas_result=True)
    
    # Return asset
    return Output(
        value = media_sources_df,
        metadata = {
            "rows": media_sources_df.index.size,
        },
    )


@asset(
    description = "Freedom convoy articles sample",
    key_prefix = ["social_signals"],
    group_name = "social_signals",
    resource_defs = {
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
    compute_kind = "snowflake",
)
def gdelt_articles(context):
    # Fetch articles
    query = f"""
        select * from social_signals.gdelt.gdelt_gkg_sample
    """
    gdelt_articles_df = context.resources.snowflake_resource.execute_query(query, fetch_results=True, use_pandas_result=True)
    
    # Return asset
    return Output(
        value = gdelt_articles_df, 
        metadata = {
            "rows": gdelt_articles_df.index.size,
        },
    )