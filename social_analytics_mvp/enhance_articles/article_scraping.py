from dagster import (
    asset,
    AssetIn,
    DailyPartitionsDefinition,
    file_relative_path,
    Output
)

import duckdb
import pandas as pd

from social_analytics_mvp.utils.resources import my_resources


social_analytics_mvp_db = file_relative_path(__file__, "./../social_analytics_mvp.db")


@asset(
    ins = {"int__articles__filter_medias": AssetIn(key_prefix="stage_sources")},
    description = "Scrape content and metadata from source articles",
    key_prefix = ["enhance_articles"],
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

    # Write df to duckdb
    connection = duckdb.connect(database=social_analytics_mvp_db)
    connection.execute("create schema if not exists enhanced_articles")

    # Check if the table exists
    table_check = connection.execute("select * from duckdb_tables() where schema_name = 'enhanced_articles' and table_name = 'articles'").fetchone()

    if not table_check:
        # if the table doesn't exist, create it from the dataframe
        connection.execute("create table enhanced_articles.articles as select * from articles_enhanced_df")
    else:
        # if the table exists
        # first, create a temporary table from your new data
        connection.execute("create temporary table temp_articles as select * from articles_enhanced_df")
        
        # update existing rows in the main table based on article_url
        connection.execute(
            """
            update enhanced_articles.articles
            set 
                file_name = temp_articles.file_name,
                title = temp_articles.title,
                description = temp_articles.description,
                keywords = temp_articles.keywords,
                content = temp_articles.content
            from temp_articles
            where enhanced_articles.articles.article_url = temp_articles.article_url
            """
        )

        # now, insert the new rows from the temporary table that don't already exist in the main table
        connection.execute(
            """
            insert into enhanced_articles.articles
            select temp_articles.*
            from temp_articles
            left join enhanced_articles.articles on temp_articles.article_url = enhanced_articles.articles.article_url
            where enhanced_articles.articles.article_url is null
            """
        )

        connection.execute("drop table temp_articles")

    # Return asset
    return Output(
        value = articles_df, 
        metadata = {
            "rows": articles_df.index.size
        }
    )