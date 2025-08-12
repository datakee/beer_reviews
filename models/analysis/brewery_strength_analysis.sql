-- Ranks breweries by average ABV to identify which breweries produce the strongest beers.
{{ config(materialized='table') }}

with brewery_abv as ( 