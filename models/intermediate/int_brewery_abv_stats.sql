{{ config(materialized='view') }}

with brewery_abv_stats as (
    select 
        brewery_name,
        count(*) as total_reviews,
        count(distinct beer_name) as distinct_beers,
        avg(beer_abv) as avg_abv,
        max(beer_abv) as max_abv,
        min(beer_abv) as min_abv,
        stddev(beer_abv) as stddev_abv
    from {{ref('stg_beer_reviews')}}
    where beer_abv is not null
      and beer_abv > 0  -- Filter out invalid ABV values
    group by brewery_name
    having count(*) >= 5)  -- Only breweries with at least 5 beers for statistical significance

select * from brewery_abv_stats
--where avg_abv < 18
order by avg_abv desc