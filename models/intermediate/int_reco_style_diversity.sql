{{ config(materialized='view') }}

/*
RECOMMENDATION STRATEGY: Style Diversity & Exploration

METHODOLOGY:
This model ensures recommendation diversity by selecting the top-rated beer from 
different beer styles, providing variety in the final recommendation set. Rather 
than potentially recommending 3 similar IPAs or stouts, this approach guarantees 
exposure to different flavor profiles and brewing traditions.

BUSINESS LOGIC:
- Identifies the highest-rated beer within each beer style category
- Ranks styles by the quality of their top representative beer
- Selects top 3 beers ensuring each represents a different style
- Uses composite scoring across multiple dimensions for style champions
- Balances style representation with overall quality

TARGET AUDIENCE:
Perfect for adventurous beer drinkers who want to explore different styles and 
flavor profiles, or for creating diverse beer flights and tasting experiences

PROS:
- Guarantees variety and prevents style bias in recommendations
- Introduces customers to different beer categories they might not normally try
- Great for educational purposes and expanding palates
- Reduces risk of recommending overly similar beers
- Excellent for gift recommendations when recipient preferences are unknown

CONS:
- May sacrifice some absolute quality for diversity goals
- The "best" beer overall might not be recommended if its style is beaten by another
- More complex to explain than single-metric approaches
- Style categorization might not align with personal taste preferences
- Could recommend a style that customer dislikes even if it's objectively excellent

SELECTION PROCESS:
1. Calculate overall ratings and composite scores for all beers
2. Rank beers within each style (partition by beer_style)
3. Select only the #1 beer from each style (style_rank = 1)
4. Rank these style champions by overall quality
5. Select top 3 style champions as final recommendations

RANKING METHODOLOGY:
- Within Style: avg_overall_rating DESC, review_count DESC
- Across Styles: avg_overall_rating DESC, composite_score DESC, review_count DESC

VALIDATION CRITERIA:
- Minimum 10 reviews per beer for reliability
- Each recommendation must represent a different beer style
- Style champions selected based on multiple quality dimensions
*/

with beer_ratings as (
    select * from {{ ref('stg_beer_reviews') }}
),

style_analysis as (
    select 
        beer_name,
        brewery_name,
        beer_style,
        beer_abv,
        avg(review_overall) as avg_overall_rating,
        avg(review_aroma) as avg_aroma,
        avg(review_taste) as avg_taste,
        avg(review_appearance) as avg_appearance,
        avg(review_palate) as avg_palate,
        count(*) as review_count
    from beer_ratings
    where review_overall is not null
      and beer_style is not null
    group by beer_name, brewery_name, beer_style, beer_abv
    having count(*) >= 10  -- Minimum reviews for reliability
),

top_by_style as (
    select 
        *,
        row_number() over (
            partition by beer_style 
            order by avg_overall_rating desc, review_count desc
        ) as style_rank,
        -- Calculate composite score for overall ranking
        (avg_aroma + avg_taste + avg_appearance + avg_palate) / 4 as composite_score
    from style_analysis
),

style_diversity_selection as (
    select 
        *,
        row_number() over (
            order by 
                case when style_rank = 1 then avg_overall_rating else 0 end desc,
                composite_score desc,
                review_count desc
        ) as diversity_rank
    from top_by_style
    where style_rank = 1  -- Only the top beer from each style
),

final_selection as (
    select 
        *,
        case when diversity_rank <= 3 then 'RECOMMENDED' else 'NOT_RECOMMENDED' end as recommendation_status
    from style_diversity_selection
)

select 
    beer_name,
    brewery_name,
    beer_style,
    beer_abv,
    round(avg_overall_rating, 3) as avg_overall_rating,
    round(avg_aroma, 3) as avg_aroma,
    round(avg_taste, 3) as avg_taste,
    round(avg_appearance, 3) as avg_appearance,
    round(avg_palate, 3) as avg_palate,
    round(composite_score, 3) as composite_score,
    review_count,
    style_rank,
    diversity_rank,
    recommendation_status
from final_selection
order by diversity_rank 