-- Descriptive statistics for the beer review dataset

SELECT 
    -- Volume metrics
    COUNT(*) as total_reviews,
    COUNT(DISTINCT beer_name) as unique_beers,
    COUNT(DISTINCT brewery_name) as unique_breweries,
    COUNT(DISTINCT beer_style) as unique_beer_styles,
    
    -- Time period coverage
    MIN(TO_TIMESTAMP(review_time)) as earliest_review,
    MAX(TO_TIMESTAMP(review_time)) as latest_review,
    DATEDIFF('year', MIN(TO_TIMESTAMP(review_time)), MAX(TO_TIMESTAMP(review_time))) + 1 as years_covered,
    COUNT(DISTINCT DATE_TRUNC('month', TO_TIMESTAMP(review_time))) as unique_months,
    
    -- Rating statistics
    AVG(review_overall) as avg_overall_rating,
    MIN(review_overall) as min_overall_rating,
    MAX(review_overall) as max_overall_rating,
    STDDEV(review_overall) as stddev_overall_rating,
    
    -- ABV statistics
    AVG(beer_abv) as avg_abv,
    MIN(beer_abv) as min_abv,
    MAX(beer_abv) as max_abv,
    
    -- Data quality
    COUNT(*) - COUNT(review_overall) as missing_overall_ratings,
    COUNT(*) - COUNT(beer_abv) as missing_abv_values,
    COUNT(*) - COUNT(review_time) as missing_timestamps

FROM {{ ref('stg_beer_reviews') }} 