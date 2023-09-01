from dagster import (
    asset,
    AssetIn,
    DailyPartitionsDefinition,
    Output
)

import pandas as pd
import hashlib

from social_analytics_mvp.utils.resources import my_resources


# Helper function to compute hash
def compute_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()


@asset(
    ins = {"int__articles__filter_medias": AssetIn(key_prefix="stage_sources")},
    description = "Scrape content and metadata from source articles",
    key_prefix = ["dp_data_prep"],
    resource_defs = {
        'web_scraper_resource': my_resources.my_web_scraper_resource
    },
    partitions_def=DailyPartitionsDefinition(
        start_date='2022-01-15',
        end_date='2022-02-28'
    ),
    compute_kind="python",
)
def articles_prep(context, int__articles__filter_medias):
    # Get partition
    partition_date_str = context.asset_partition_key_for_output()

    # Keep partition's articles and dedup articles
    articles_df = int__articles__filter_medias[int__articles__filter_medias['article_ts'].dt.date == pd.Timestamp(partition_date_str).date()]
    articles_df = articles_df.drop_duplicates(subset=["article_url"], keep='first')

    # Create dataframe
    column_names = ['article_url', 'file_name', 'title', 'description', 'keywords', 'content']
    articles_enhanced_df = pd.DataFrame(columns = column_names)

    for _, row in articles_df.iterrows():
        try:
            scraped_article = context.resources.web_scraper_resource.scrape_article(row["article_url"])
        except IndexError:
            break
        except ConnectionResetError:
            break

        if scraped_article is None:
            continue
    
        # Use get method with default value (empty string) for each element in scraped_row
        scraped_row = [
            scraped_article.get('url', ''),
            scraped_article.get('filename', ''),
            scraped_article.get('title', ''),
            scraped_article.get('description', ''),
            scraped_article.get('keywords', ''),
            scraped_article.get('content', '')
        ]
        
        df_length = len(articles_enhanced_df)
        articles_enhanced_df.loc[df_length] = scraped_row

    # Return asset
    return Output(
        value = articles_df, 
        metadata = {
            "rows": articles_df.index.size
        }
    )