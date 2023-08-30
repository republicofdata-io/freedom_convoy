with base as (

    select
        article_url,
        article_media_source,
        themes,
        locations,
        persons,
        organizations,
        article_date,
        partition_ts as gdelt_partition_ts
        
    from {{ ref('gdelt_articles') }}

)

select * from base