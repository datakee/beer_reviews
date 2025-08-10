{{ config(materialized='table') }}

/*
SEASONALITY RESULTS: All decomposition data for visualization

Simple pass-through of seasonality decomposition results
Filter by chart_type in Tableau for different views
*/

select *
from {{ ref('seasonality_decomposition') }} 