{{ config(materialized='table') }}

-- Selects results from the seasonality decomposition analysis for visualization.
select *
from {{ ref('seasonality_decomposition') }} 