{{ config(materialized='view') }}

with top_1_style as (
    select beer_style
    from (
        select 
            beer_style,
            AVG(review_overall) as avg_overall_rating,
            COUNT(*) as total_reviews
        from {{ ref('stg_beer_reviews') }}
        where beer_style is not null
          and review_overall is not null
        group by beer_style
        having COUNT(*) >= 1000
        order by avg_overall_rating desc
        limit 1
    )
),

regular_beers as (
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
    left join top_1_style t on s.beer_style = t.beer_style
    where t.beer_style is null  -- Not in top 1 style
      and s.beer_abv <= 10.0    -- Not strong beers
      and s.beer_abv > 0         -- Valid ABV
      and s.beer_abv is not null
      and s.review_overall is not null
      and s.review_aroma is not null
      and s.review_taste is not null
      and s.review_appearance is not null
      and s.review_palate is not null
)

select * from regular_beers 