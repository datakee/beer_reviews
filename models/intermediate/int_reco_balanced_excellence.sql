{{ config(materialized='view') }}

/*
RECOMMENDATION STRATEGY: Balanced Excellence Across All Dimensions

METHODOLOGY:
This model identifies beers that perform exceptionally well across ALL rating dimensions
(aroma, taste, appearance, palate) rather than just overall rating. It seeks "complete" 
beers with no weak points, ensuring a consistently excellent experience across all 
sensory aspects.

BUSINESS LOGIC:
- Calculates a composite "balanced_score" by averaging aroma, taste, appearance, and palate ratings
- Identifies the "weakest link" (minimum dimension score) to avoid beers with any poor aspects
- Measures "dimension_consistency" to prefer beers with uniform excellence vs. uneven performance
- Requires complete data across all four dimensions for fair comparison
- Higher review threshold (15+) due to more stringent data requirements

TARGET AUDIENCE:
Perfect for discerning beer enthusiasts who want excellence across all sensory experiences
and aren't willing to compromise on any aspect of beer quality

PROS:
- Identifies truly exceptional beers with no weak points
- Appeals to sophisticated palates seeking complete experiences
- Reduces risk of disappointment in any particular aspect
- More comprehensive quality assessment than single-metric approaches

CONS:
- Excludes beers that might be outstanding in one dimension but average in others
- Higher data requirements may eliminate some excellent beers with fewer reviews
- May be overly conservative and miss innovative or distinctive beers
- Complex methodology may be harder to explain to stakeholders

RANKING METHODOLOGY:
1. Primary: balanced_score DESC (average of all 4 dimensions)
2. Secondary: min_dimension_score DESC (strength of weakest aspect)
3. Tertiary: review_count DESC (statistical reliability)

VALIDATION CRITERIA:
- Each beer must have ≥15 reviews with complete data across all dimensions
- No beer recommended if any single dimension scores below average
*/

with beer_ratings as (
    select * from {{ ref('stg_beer_reviews') }}
),

balanced_ratings as (
    select 
        beer_name,
        brewery_name,
        beer_style,
        beer_abv,
        avg(review_overall) as avg_overall,
        avg(review_aroma) as avg_aroma,
        avg(review_taste) as avg_taste,
        avg(review_appearance) as avg_appearance,
        avg(review_palate) as avg_palate,
        count(*) as review_count
    from beer_ratings
    where review_overall is not null
      and review_aroma is not null
      and review_taste is not null
      and review_appearance is not null
      and review_palate is not null
    group by beer_name, brewery_name, beer_style, beer_abv
    having count(*) >= 15  -- Higher threshold for balanced analysis
),

balanced_scores as (
    select 
        *,
        -- Calculate composite balanced score (average of all dimensions)
        (avg_aroma + avg_taste + avg_appearance + avg_palate) / 4 as balanced_score,
        -- Calculate minimum dimension score (weakest link)
        least(avg_aroma, avg_taste, avg_appearance, avg_palate) as min_dimension_score,
        -- Calculate standard deviation across dimensions (consistency measure)
        sqrt(
            (power(avg_aroma - ((avg_aroma + avg_taste + avg_appearance + avg_palate) / 4), 2) +
             power(avg_taste - ((avg_aroma + avg_taste + avg_appearance + avg_palate) / 4), 2) +
             power(avg_appearance - ((avg_aroma + avg_taste + avg_appearance + avg_palate) / 4), 2) +
             power(avg_palate - ((avg_aroma + avg_taste + avg_appearance + avg_palate) / 4), 2)) / 4
        ) as dimension_consistency
    from balanced_ratings
),

ranked_balanced as (
    select 
        *,
        row_number() over (order by balanced_score desc, min_dimension_score desc, review_count desc) as balanced_rank
    from balanced_scores
)

select 
    beer_name,
    brewery_name,
    beer_style,
    beer_abv,
    round(avg_overall, 3) as avg_overall,
    round(avg_aroma, 3) as avg_aroma,
    round(avg_taste, 3) as avg_taste,
    round(avg_appearance, 3) as avg_appearance,
    round(avg_palate, 3) as avg_palate,
    round(balanced_score, 3) as balanced_score,
    round(min_dimension_score, 3) as min_dimension_score,
    round(dimension_consistency, 3) as dimension_consistency,
    review_count,
    balanced_rank,
    case when balanced_rank <= 3 then 'RECOMMENDED' else 'NOT_RECOMMENDED' end as recommendation_status
from ranked_balanced
order by balanced_rank 