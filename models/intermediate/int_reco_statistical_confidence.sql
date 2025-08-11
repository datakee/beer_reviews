{{ config(materialized='view') }}

with beer_ratings as (
    select * from {{ ref('stg_beer_reviews') }}
),

statistical_analysis as (
    select 
        beer_name,
        brewery_name,
        beer_style,
        beer_abv,
        avg(review_overall) as avg_rating,
        count(*) as review_count,
        stddev(review_overall) as rating_stddev,
        min(review_overall) as min_rating,
        max(review_overall) as max_rating
    from beer_ratings
    where review_overall is not null
    group by beer_name, brewery_name, beer_style, beer_abv
    having count(*) >= 20  -- High review threshold for statistical confidence
),

confidence_scores as (
    select 
        *,
        -- Confidence score: rating weighted by logarithm of review count
        avg_rating * ln(review_count) as confidence_score
    from statistical_analysis
),

ranked_confidence as (
    select 
        *,
        row_number() over (order by confidence_score desc) as confidence_rank,
        row_number() over (order by avg_rating desc, review_count desc) as simple_rank
    from confidence_scores
),

final as (select 
    beer_name,
    brewery_name,
    beer_style,
    beer_abv,
    round(avg_rating, 3) as avg_rating,
    review_count,
    round(rating_stddev, 3) as rating_stddev,
    round(confidence_score, 3) as confidence_score,
    confidence_rank,
    simple_rank,
    case when confidence_rank <= 3 then 'RECOMMENDED' else 'NOT_RECOMMENDED' end as recommendation_status
from ranked_confidence
order by confidence_rank)

select 
    *
from
    final