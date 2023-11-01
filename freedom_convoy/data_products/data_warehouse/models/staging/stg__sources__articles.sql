with base as (

    select
        article_url,
        article_media_source,
        themes,
        locations,
        persons,
        organizations,
        strptime(article_date, '%Y%m%d%H%M%S') as article_ts,
        partition_ts as gdelt_partition_ts
        
    from {{ source('social_signals', 'gdelt_articles') }}

)

select * from base