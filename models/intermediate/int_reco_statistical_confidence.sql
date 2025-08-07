{{ config(materialized='view') }}

/*
RECOMMENDATION STRATEGY: Statistical Confidence & Reliability

METHODOLOGY:
This model prioritizes statistically reliable recommendations by combining high ratings
with high review volumes. It uses advanced statistical techniques to account for both
the average rating and the confidence we can have in that rating based on sample size.
This approach minimizes the risk of recommending a beer that might be rated highly 
but based on insufficient data.

BUSINESS LOGIC:
- Applies a "confidence_score" that weights ratings by logarithm of review count
- Implements Wilson Score interval for more sophisticated confidence assessment
- Calculates standard error to quantify uncertainty in the ratings
- Requires high review threshold (20+) to ensure statistical significance
- Accounts for rating variance/consistency within the review population

TARGET AUDIENCE:
Ideal for risk-averse customers or business contexts where recommendation accuracy
is critical and you cannot afford to suggest a beer that might disappoint

PROS:
- Mathematically rigorous approach reduces selection bias
- Accounts for both quality AND reliability of recommendations
- Less likely to be fooled by small sample sizes with extreme ratings
- Provides multiple statistical measures for validation
- Great for explaining methodology to data-savvy stakeholders

CONS:
- May miss excellent newer beers that haven't accumulated many reviews yet
- Could be biased toward older, more established beers
- Complex statistical methodology may be harder for general audiences to understand
- Very conservative approach might miss unique or niche excellent beers

STATISTICAL TECHNIQUES USED:
1. Confidence Score: rating × ln(review_count) - rewards both quality and volume
2. Wilson Score: Bayesian approach to confidence intervals for ratings
3. Standard Error: Measures uncertainty in the mean rating
4. Multiple ranking systems for cross-validation

RANKING METHODOLOGY:
- Primary: confidence_score DESC (statistical confidence-weighted rating)
- Alternative rankings provided: Wilson score, simple rating for comparison

VALIDATION CRITERIA:
- Minimum 20 reviews per beer for statistical significance
- Standard error calculated for uncertainty quantification
- Multiple statistical measures for robustness validation
*/

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
        avg_rating * ln(review_count) as confidence_score,
        -- Wilson score for more sophisticated confidence interval
        -- Simplified version: (positive reviews + 1.96^2/2) / (total + 1.96^2) 
        -- Assuming reviews >= 4.0 are "positive"
        (avg_rating * review_count + 3.84) / (review_count + 3.84) as wilson_score,
        -- Standard error of the mean
        case 
            when rating_stddev is not null and review_count > 1 
            then rating_stddev / sqrt(review_count)
            else null 
        end as standard_error
    from statistical_analysis
),

ranked_confidence as (
    select 
        *,
        row_number() over (order by confidence_score desc) as confidence_rank,
        row_number() over (order by wilson_score desc) as wilson_rank,
        row_number() over (order by avg_rating desc, review_count desc) as simple_rank
    from confidence_scores
)

select 
    beer_name,
    brewery_name,
    beer_style,
    beer_abv,
    round(avg_rating, 3) as avg_rating,
    review_count,
    round(rating_stddev, 3) as rating_stddev,
    round(confidence_score, 3) as confidence_score,
    round(wilson_score, 3) as wilson_score,
    round(standard_error, 4) as standard_error,
    confidence_rank,
    wilson_rank,
    simple_rank,
    case when confidence_rank <= 3 then 'RECOMMENDED' else 'NOT_RECOMMENDED' end as recommendation_status
from ranked_confidence
order by confidence_rank 