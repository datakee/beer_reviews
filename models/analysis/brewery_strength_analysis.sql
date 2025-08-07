{{ config(materialized='table') }}

-- Analysis: Which brewery produces the strongest beers by ABV%?

with beer_data as (
    select * from {{ ref('stg_beer_reviews') }}
),

brewery_abv_stats as (
    select * from {{ ref('int_brewery_abv_stats') }}
),

brewery_rankings as (
    select 
        brewery_name,
        total_beers,
        round(avg_abv, 2) as avg_abv,
        round(max_abv, 2) as max_abv,
        round(min_abv, 2) as min_abv,
        round(stddev_abv, 2) as stddev_abv,
        row_number() over (order by avg_abv desc) as rank_by_avg_abv,
        row_number() over (order by max_abv desc) as rank_by_max_abv
    from brewery_abv_stats
)

select * from brewery_rankings
order by avg_abv desc 