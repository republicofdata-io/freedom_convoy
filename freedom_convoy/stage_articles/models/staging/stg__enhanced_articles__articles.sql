with base as (

    select *
        
    from {{ source('enhanced_articles', 'articles') }}

)

select * from base