{{ config(materialized='table') }}

/*
NON-ALCOHOLIC BEER RANKINGS: Top recommendations for alcohol-free portfolio segment

Simple ranking of highest-rated non-alcoholic beers by average rating.
*/

with non_alcoholic_beers as (
    select 
        beer_name,
        brewery_name,
        beer_style,
        avg(review_overall) as avg_rating,
        count(*) as review_count
    from {{ ref('stg_beer_reviews') }}
    where beer_abv < 0.5  -- Non-alcoholic threshold
      and review_overall is not null
    group by beer_name, brewery_name, beer_style
    having count(*) >= 10  -- Minimum reviews for reliability
),

ranked_non_alcoholic as (
    select 
        *,
        row_number() over (order by avg_rating desc) as rank
    from non_alcoholic_beers
)

select 
    beer_name,
    brewery_name,
    beer_style,
    round(avg_rating, 3) as avg_rating,
    review_count,
    rank
from ranked_non_alcoholic
order by rank 