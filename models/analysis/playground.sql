{{ config(materialized='table') }}

with beer_data as (
    select * from {{ ref('stg_beer_reviews') }}
    where beer_abv is not null
    and beer_abv > 0
),

agg as (select
    brewery_name,
    count(*) as review_count,
    avg(beer_abv)
from beer_data
    group by 1
    order by 3 desc
),

beers as (
    select
        distinct beer_name,
        beer_abv,
        count(*) as review_count
    from beer_data
    where brewery_name = 'Schorschbräu'
    group by 1,2
)

select * from beers