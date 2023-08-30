with base as (

    select
        "News Source" as news_source,
        domain,
        "Country" as country,
        "Media Type" as media_type,
        "Bias" as bias,
        "Credibility" as credibility,
        "Reporting" as reporting,
        "Traffic" as traffic,
        "Popularity Estimate" as popularity_estimate

    from analytics_sources.media_sources

)

select * from base