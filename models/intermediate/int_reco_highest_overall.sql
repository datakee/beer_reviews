{{ config(materialized='view') }}

/*
RECOMMENDATION STRATEGY: Highest Overall Rating

METHODOLOGY:
This model identifies the top 3 beer recommendations based on the highest average overall 
rating scores from reviewers. This is the most straightforward approach - simply finding 
the beers that consistently receive the highest overall ratings.

BUSINESS LOGIC:
- Focuses purely on the "review_overall" field as the primary quality indicator
- Requires minimum 10 reviews per beer to ensure statistical reliability
- Uses review count as a tiebreaker when overall ratings are equal
- Assumes that reviewers' overall ratings are the best single indicator of beer quality

TARGET AUDIENCE:
Best for customers who want proven crowd-pleasers with broad appeal

PROS:
- Simple, intuitive methodology that's easy to explain
- Directly reflects customer satisfaction
- Clear ranking system

CONS:
- May miss beers that excel in specific dimensions but have average overall scores
- Could be biased toward mainstream styles that appeal to many reviewers
- Doesn't account for personal taste preferences across different beer characteristics

VALIDATION CRITERIA:
- Each recommended beer must have ≥10 reviews for reliability
- Rankings based on: (1) avg_overall_rating DESC, (2) review_count DESC
*/

with beer_ratings as (
    select * from {{ ref('stg_beer_reviews') }}
),

aggregated_ratings as (
    select 
        beer_name,
        brewery_name,
        beer_style,
        beer_abv,
        avg(review_overall) as avg_overall_rating,
        count(*) as review_count,
        stddev(review_overall) as rating_stddev
    from beer_ratings
    where review_overall is not null
    group by beer_name, brewery_name, beer_style, beer_abv
    having count(*) >= 10  -- Minimum 10 reviews for reliability
),

ranked_beers as (
    select 
        *,
        row_number() over (order by avg_overall_rating desc, review_count desc) as overall_rank
    from aggregated_ratings
)

select 
    beer_name,
    brewery_name,
    beer_style,
    beer_abv,
    round(avg_overall_rating, 3) as avg_overall_rating,
    review_count,
    round(rating_stddev, 3) as rating_stddev,
    overall_rank,
    case when overall_rank <= 3 then 'RECOMMENDED' else 'NOT_RECOMMENDED' end as recommendation_status
from ranked_beers
order by overall_rank 