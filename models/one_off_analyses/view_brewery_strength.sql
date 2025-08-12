-- View top brewery by average ABV strength

SELECT 
    brewery_name,
    avg_abv,
    total_beers,
    rank_by_avg_abv
FROM {{ ref('brewery_strength_analysis') }}
WHERE rank_by_avg_abv = 1
LIMIT 1 