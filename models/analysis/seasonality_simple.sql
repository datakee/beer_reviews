-- Aggregates reviews by month, year, 
--and beer style for basic seasonality exploration in Tableau.
{{ config(materialized='table') }}

-- Simple seasonality analysis: monthly aggregations by beer style

with monthly_aggregations as (
    select 
        DATE_TRUNC('month', TO_TIMESTAMP(review_time)) as month_year,
        EXTRACT(year FROM TO_TIMESTAMP(review_time)) as year,
        EXTRACT(month FROM TO_TIMESTAMP(review_time)) as month,
        beer_style,
        
        -- Aggregated metrics
        AVG(review_overall) as avg_overall_rating,
        AVG(review_aroma) as avg_aroma,
        AVG(review_taste) as avg_taste,
        AVG(review_appearance) as avg_appearance,
        AVG(review_palate) as avg_palate,
        AVG(beer_abv) as avg_abv,
        
        -- Volume metrics
        COUNT(*) as review_count,
        COUNT(DISTINCT beer_name) as unique_beers,
        COUNT(DISTINCT brewery_name) as unique_breweries
        
    from {{ ref('stg_beer_reviews') }}
    where review_time IS NOT NULL 
      and review_overall IS NOT NULL
      and beer_style IS NOT NULL
    group by 1, 2, 3, 4
)

select * from monthly_aggregations
order by year, month, beer_style 