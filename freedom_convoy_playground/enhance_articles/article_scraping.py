from dagster import (
    asset,
    AssetIn,
    DailyPartitionsDefinition,
    file_relative_path,
    Output
)

import duckdb
from langchain.chains import (
    LLMChain,
    SequentialChain
)
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from openai.error import RateLimitError
import pandas as pd
import time

from freedom_convoy_playground.utils.resources import my_resources


freedom_convoy_playground_db = file_relative_path(__file__, "./../freedom_convoy_playground.db")

def retry_llm_execution(llm_chain, max_retries=3, delay=5):
    """Attempt to execute the LLM chain and retry on rate limit errors."""
    for _ in range(max_retries):
        try:
            return llm_chain({})
        except RateLimitError:
            time.sleep(delay)
    raise RateLimitError("Max retries hit, still facing RateLimitError.")


@asset(
    ins = {"int__articles__filter_medias": AssetIn(key_prefix="stage_sources")},
    description = "Scrape content and metadata from source articles",
    key_prefix = ["enhance_articles"],
    resource_defs = {
        'web_scraper_resource': my_resources.my_web_scraper_resource
    },
    partitions_def=DailyPartitionsDefinition(
        start_date='2022-01-15',
        end_date='2022-03-01'
    ),
    compute_kind="python",
)
def article_scraped_data(context, int__articles__filter_medias):
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
    connection = duckdb.connect(database=freedom_convoy_playground_db)
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
        value = articles_enhanced_df, 
        metadata = {
            "rows": articles_enhanced_df.index.size
        }
    )


@asset(
    ins = {"article_scraped_data": AssetIn(key_prefix="enhance_articles")},
    description = "Use LLM to filter and extract additional info",
    key_prefix = ["enhance_articles"],
    partitions_def=DailyPartitionsDefinition(
        start_date='2022-01-15',
        end_date='2022-03-01'
    ),
    compute_kind="LangChain",
)
def article_llm_enhancements(context, article_scraped_data):
    # Get partition
    partition_date_str = context.asset_partition_key_for_output()

    # Cycle through each article and enhance
    article_llm_enhancements_df = pd.DataFrame(columns = ['article_url', 'summary', 'relevancy'])
    for _, row in article_scraped_data.iterrows():
        # LLM chain to get a summary of the article
        summary_template = f"""You are a helpful assistant who generates concise summaries based on article's title, description and content.
        #####
        Title: {row['title'][:100]}
        Description: {row['description'][:500]}
        Content: {row['content'][:1500]}
        """
        summary_prompt_template = PromptTemplate(input_variables=[], template=summary_template)

        summary_chain = LLMChain(
            llm=ChatOpenAI(
                model_name="gpt-3.5-turbo",
                max_tokens=1500,
                temperature=0
            ),
            prompt=summary_prompt_template,
            output_key="summary"
        )

        # LLM chain to get assess relevancy of the article
        relevancy_template = """Given this overview of the Canada Convoy Protest: 
        'A series of protests and blockades in Canada against COVID-19 vaccine mandates and restrictions, called the Freedom Convoy (French: Convoi de la liberté) by organizers, began in early 2022. The initial convoy movement was created to protest vaccine mandates for crossing the United States border, but later evolved into a protest about COVID-19 mandates in general. Beginning January 22, hundreds of vehicles formed convoys from several points and traversed Canadian provinces before converging on Ottawa on January 29, 2022, with a rally at Parliament Hill. The convoys were joined by thousands of pedestrian protesters. Several offshoot protests blockaded provincial capitals and border crossings with the United States.'
        Can you tell me if the following article talks about those protests, either the event itself, the actors, the aftermaths, etc. Answer strictly by either 'True' or 'False'.
        #####
        Article summary: {summary}
        """
        relevancy_prompt_template = PromptTemplate(input_variables=["summary"], template=relevancy_template)

        relevancy_chain = LLMChain(
            llm=ChatOpenAI(
                model_name="gpt-3.5-turbo",
                max_tokens=1500,
                temperature=0
            ),
            prompt=relevancy_prompt_template,
            output_key="relevancy"
        )

        # Put together the overall chain and run
        overall_chain = SequentialChain(
            chains=[summary_chain, relevancy_chain],
            input_variables=[],
            output_variables=["summary", "relevancy"],
            verbose=True)
        
        overall_completion_str = retry_llm_execution(overall_chain)

        df_length = len(article_llm_enhancements_df)
        article_llm_enhancements_df.loc[df_length] = [row['article_url'], overall_completion_str['summary'], overall_completion_str['relevancy']]

        time.sleep(5)

    # Write df to duckdb
    connection = duckdb.connect(database=freedom_convoy_playground_db)
    connection.execute("create schema if not exists enhanced_articles")

    # Check if the table exists
    table_check = connection.execute("select * from duckdb_tables() where schema_name = 'enhanced_articles' and table_name = 'article_llm_enhancements'").fetchone()

    if not table_check:
        # if the table doesn't exist, create it from the dataframe
        connection.execute("create table enhanced_articles.article_llm_enhancements as select * from article_llm_enhancements_df")
    else:
        # if the table exists
        # first, create a temporary table from your new data
        connection.execute("create temporary table temp_article_llm_enhancements as select * from article_llm_enhancements_df")
        
        # update existing rows in the main table based on article_url
        connection.execute(
            """
            update enhanced_articles.article_llm_enhancements
            set 
                article_url = temp_article_llm_enhancements.article_url,
                summary = temp_article_llm_enhancements.summary,
                relevancy = temp_article_llm_enhancements.relevancy
            from temp_article_llm_enhancements
            where enhanced_articles.article_llm_enhancements.article_url = temp_article_llm_enhancements.article_url
            """
        )

        # now, insert the new rows from the temporary table that don't already exist in the main table
        connection.execute(
            """
            insert into enhanced_articles.article_llm_enhancements
            select temp_article_llm_enhancements.*
            from temp_article_llm_enhancements
            left join enhanced_articles.article_llm_enhancements on temp_article_llm_enhancements.article_url = enhanced_articles.article_llm_enhancements.article_url
            where enhanced_articles.article_llm_enhancements.article_url is null
            """
        )

        connection.execute("drop table temp_article_llm_enhancements")

    # Return asset
    return Output(
        value = article_llm_enhancements_df, 
        metadata = {
            "rows": article_llm_enhancements_df.index.size
        }
    )