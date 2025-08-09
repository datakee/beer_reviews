{{ config(materialized='view') }}

with beer_style_ratings as (
    select 
        beer_style,
        AVG(review_overall) as avg_overall_rating,
        COUNT(*) as total_reviews,
        COUNT(DISTINCT beer_name) as unique_beers
    from {{ ref('stg_beer_reviews') }}
    where beer_style is not null
      and review_overall is not null
    group by beer_style
    having COUNT(*) >= 1000  -- Only styles with at least 1000 reviews
),

top_1_style as (
    select beer_style
    from beer_style_ratings
    order by avg_overall_rating desc
    limit 1
),

filtered_reviews as (
    select 
        s.beer_name,
        s.brewery_name,
        s.beer_style,
        s.beer_abv,
        s.review_overall,
        s.review_aroma,
        s.review_taste,
        s.review_appearance,
        s.review_palate,
        s.review_time,
        s.review_datetime
    from {{ ref('stg_beer_reviews') }} s
    inner join top_1_style t on s.beer_style = t.beer_style
    where s.review_overall is not null
      and s.review_aroma is not null
      and s.review_taste is not null
      and s.review_appearance is not null
      and s.review_palate is not null
)

select * from filtered_reviews 