{{ config(materialized='view') }}

with strong_beers as (
    select 
        beer_name,
        brewery_name,
        beer_style,
        beer_abv,
        review_overall,
        review_aroma,
        review_taste,
        review_appearance,
        review_palate,
        review_time,
        review_datetime
    from {{ ref('stg_beer_reviews') }}
    where beer_abv > 10.0
      and beer_abv is not null
      and review_overall is not null
      and review_aroma is not null
      and review_taste is not null
      and review_appearance is not null
      and review_palate is not null
)

select * from strong_beers 