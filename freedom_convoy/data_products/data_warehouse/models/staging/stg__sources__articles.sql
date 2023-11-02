with base as (

    select
        article_url,
        article_media_source,
        themes,
        locations,
        persons,
        organizations,
        to_timestamp(to_varchar(article_date), 'yyyymmddhh24miss') as article_ts,
        partition_ts as gdelt_partition_ts
        
    from {{ source('social_signals', 'gdelt_articles') }}

)

select * from base