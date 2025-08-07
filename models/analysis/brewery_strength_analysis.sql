{{ config(materialized='table') }}

-- Analysis: Which brewery produces the strongest beers by ABV%?

with beer_data as (
    select * from {{ ref('stg_beer_reviews') }}
),

brewery_abv_stats as (
    select 
        brewery_name,
        count(*) as total_beers,
        avg(beer_abv) as avg_abv,
        max(beer_abv) as max_abv,
        min(beer_abv) as min_abv,
        stddev(beer_abv) as stddev_abv
    from beer_data
    where beer_abv is not null
      and beer_abv > 0  -- Filter out invalid ABV values
    group by brewery_name
    having count(*) >= 5  -- Only breweries with at least 5 beers for statistical significance
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