-- Selects results from the seasonality by 
-- style analysis for visualization.
select *
from {{ ref('seasonality_by_style') }}
order by 3, 1  -- order by beer_style, month 