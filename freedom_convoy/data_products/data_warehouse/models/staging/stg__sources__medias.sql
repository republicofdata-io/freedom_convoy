with base as (

    select
        news_source,
        domain,
        country,
        media_type,
        bias,
        credibility,
        reporting,
        traffic,
        popularity_estimate

    from {{ source('social_signals', 'media_sources') }}

)

select * from base