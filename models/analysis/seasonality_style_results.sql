{{ config(materialized='table') }}

/*
SEASONALITY BY BEER STYLE: Seasonal Index Results

Displays seasonal indices for Imperial IPA vs Imperial Stout
- seasonal_index > 1.0 = above average month (peak season)
- seasonal_index < 1.0 = below average month (low season)  
- seasonal_index = 1.0 = average month

For Tableau visualization of seasonal differences between key portfolio styles
*/

select *
from {{ ref('seasonality_by_style') }}
order by 3, 1  -- order by beer_style, month 