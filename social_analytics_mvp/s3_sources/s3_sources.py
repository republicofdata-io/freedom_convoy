from dagster import (
    asset,
    file_relative_path,
    Output
)
from dagster_aws.s3 import S3Resource

import duckdb
import pandas as pd
import io
import os


social_analytics_mvp_db = file_relative_path(__file__, "./../social_analytics_mvp.db")


@asset(
    description="GDELT articles",
    key_prefix=["s3_sources"],
    compute_kind="python",
)
def gdelt_articles(s3: S3Resource):
    # Download the file into a BytesIO object
    obj = s3.get_client().get_object(Bucket='social-analytics-mvp', Key='sources/gdelt_articles.csv')
    file_content = io.BytesIO(obj['Body'].read())
    
    # Read the content into a DataFrame
    gdelt_articles_df = pd.read_csv(file_content)

    # Write df to duckdb
    connection = duckdb.connect(database=social_analytics_mvp_db)
    connection.execute("create schema if not exists s3_sources")
    connection.execute(
        "create or replace table s3_sources.gdelt_articles as select * from gdelt_articles_df"
    )

    # Return asset
    return Output(
        value = gdelt_articles_df, 
        metadata = {
            "rows": gdelt_articles_df.index.size
        }
    )


@asset(
    description="Media sources",
    key_prefix=["s3_sources"],
    compute_kind="python",
)
def media_sources(s3: S3Resource):
    # Download the file into a BytesIO object
    obj = s3.get_client().get_object(Bucket='social-analytics-mvp', Key='sources/media_sources.csv')
    file_content = io.BytesIO(obj['Body'].read())
    
    # Read the content into a DataFrame
    media_sources_df = pd.read_csv(file_content)

    # Write df to duckdb
    connection = duckdb.connect(database=social_analytics_mvp_db)
    connection.execute("create schema if not exists s3_sources")
    connection.execute(
        "create or replace table s3_sources.media_sources as select * from media_sources_df"
    )

    # Return asset
    return Output(
        value = media_sources_df, 
        metadata = {
            "rows": media_sources_df.index.size
        }
    )