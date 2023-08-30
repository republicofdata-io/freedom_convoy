with articles as (

    select * from {{ ref('stg__sources__articles') }}

),

medias as (

    select * from {{ ref('stg__sources__medias') }}

),

filter_articles as (

    select *
    from articles
    where article_media_source in (
        select domain from medias
    )

)

select * from filter_articles