{{ config(materialized='table') }}

/*
QUESTION 4: ANALYSIS: Beer Style Recommendations Based on Aroma and Appearance Preferences

BUSINESS QUESTION:
"If I typically enjoy a beer due to its aroma and appearance, which beer style should I try?"

METHODOLOGY:
1. Calculate average aroma and appearance ratings by beer style
2. Rank by simple combined aroma + appearance score
3. Require minimum sample size for statistical reliability
4. Add market opportunity sizing for portfolio strategy
*/

with beer_data as (
    select * from {{ ref('stg_beer_reviews') }}
),

style_aroma_appearance_analysis as (
    select
        beer_style,
        count(*) as total_reviews,

        -- Aroma metrics
        avg(review_aroma) as avg_aroma,
        stddev(review_aroma) as stddev_aroma,
        min(review_aroma) as min_aroma,
        max(review_aroma) as max_aroma,

        -- Appearance metrics
        avg(review_appearance) as avg_appearance,
        stddev(review_appearance) as stddev_appearance,
        min(review_appearance) as min_appearance,
        max(review_appearance) as max_appearance,

        -- Combined aroma + appearance score (simple average)
        (avg(review_aroma) + avg(review_appearance)) / 2 as combined_aroma_appearance_score,

        -- Overall quality for context
        avg(review_overall) as avg_overall,
        avg(review_taste) as avg_taste,
        avg(review_palate) as avg_palate

    from beer_data
    where review_aroma is not null
      and review_appearance is not null
      and beer_style is not null
    group by beer_style
    having count(*) >= 20  -- Minimum reviews for reliability
),

style_rankings as (
    select
        *,
        -- Simple rankings
        row_number() over (order by avg_aroma desc) as aroma_rank,
        row_number() over (order by avg_appearance desc) as appearance_rank,
        row_number() over (order by combined_aroma_appearance_score desc) as combined_rank

    from style_aroma_appearance_analysis
),

final_recommendations as (
    select
        beer_style,
        total_reviews,
        round(avg_aroma, 3) as avg_aroma,
        round(avg_appearance, 3) as avg_appearance,
        round(combined_aroma_appearance_score, 3) as combined_score,
        round(avg_overall, 3) as avg_overall,
        aroma_rank,
        appearance_rank,
        combined_rank,
        
        -- Market opportunity sizing
        total_reviews as market_opportunity_size,
        round(total_reviews / sum(total_reviews) over(), 4) as pct_of_reviews,
        ntile(4) over (order by total_reviews) as market_size_quartile,
        case 
            when total_reviews >= 10000 then 'LARGE_MARKET'
            when total_reviews >= 5000 then 'MEDIUM_MARKET'
            when total_reviews >= 1000 then 'SMALL_MARKET'
            else 'NICHE_MARKET'
        end as market_size_category,
        
        -- Use simple combined score for final ranking
        row_number() over (order by combined_aroma_appearance_score desc) as final_recommendation_rank,
        case
            when row_number() over (order by combined_aroma_appearance_score desc) <= 5
            then 'HIGHLY RECOMMENDED'
            when row_number() over (order by combined_aroma_appearance_score desc) <= 10
            then 'RECOMMENDED'
            else 'CONSIDER'
        end as recommendation_category,
        
        -- Portfolio strategy classification
        case 
            -- High quality + large market = safe, profitable core holdings
            when row_number() over (order by combined_aroma_appearance_score desc) <= 5 
                 and total_reviews >= 10000 then 'CORE_PORTFOLIO'
            -- High quality + small market = low-volume, high-margin specialty offerings
            when row_number() over (order by combined_aroma_appearance_score desc) <= 5 
                 and total_reviews < 1000 then 'PREMIUM_SPECIALTY'
            -- Large market + average quality = high-volume, mass-appeal products
            when total_reviews >= 10000 then 'VOLUME_PLAY'
            -- Medium market = growth potential with manageable competition
            when total_reviews >= 5000 then 'GROWTH_OPPORTUNITY'
            -- Small market + average quality = limited strategic value
            else 'NICHE_OPPORTUNITY'
        end as portfolio_strategy
    from style_rankings
)

select * from final_recommendations
order by final_recommendation_rank 