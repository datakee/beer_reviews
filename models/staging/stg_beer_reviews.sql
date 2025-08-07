{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('beer_reviews_raw', 'beer_reviews_raw') }}
),

cleaned_data as (
    select 
        beer_name,
        brewery_name,
        review_overall,
        review_aroma,
        review_appearance,
        review_palate,
        review_taste,
        beer_style,
        beer_abv,
        review_time
    from source_data
    where beer_name is not null
      and brewery_name is not null
)

select * from cleaned_data