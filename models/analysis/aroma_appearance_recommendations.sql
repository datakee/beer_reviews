{{ config(materialized='table') }}

with beer_data as (
    select * from {{ ref('stg_beer_reviews') }}
),

style_aroma_appearance_analysis as (
    select
        beer_style,
        count(*) as total_reviews,
        avg(review_aroma) as avg_aroma,
        avg(review_appearance) as avg_appearance,
        (avg(review_aroma) + avg(review_appearance)) / 2 as combined_aroma_appearance_score,
        avg(review_overall) as avg_overall
    from beer_data
    where review_aroma is not null
      and review_appearance is not null
      and beer_style is not null
    group by beer_style
    having count(*) >= 20  -- Minimum reviews for reliability
),

ranked_styles as (
    select
        *,
        row_number() over (order by combined_aroma_appearance_score desc) as final_rank,
        ntile(4) over (order by total_reviews) as market_size_quartile
    from style_aroma_appearance_analysis
),

market_categories as (
    select
        *,
        case 
            when total_reviews >= 10000 then 'LARGE_MARKET'
            when total_reviews >= 5000 then 'MEDIUM_MARKET'
            when total_reviews >= 1000 then 'SMALL_MARKET'
            else 'NICHE_MARKET'
        end as market_size_category,
        case
            when final_rank <= 5 then 'HIGHLY RECOMMENDED'
            when final_rank <= 10 then 'RECOMMENDED'
            else 'CONSIDER'
        end as recommendation_category
    from ranked_styles
),

final_recommendations as (
    select
        beer_style,
        total_reviews,
        round(avg_aroma, 3) as avg_aroma,
        round(avg_appearance, 3) as avg_appearance,
        round(combined_aroma_appearance_score, 3) as combined_score,
        round(avg_overall, 3) as avg_overall,
        final_rank,
        round(total_reviews / sum(total_reviews) over(), 4) as pct_of_reviews,
        market_size_quartile,
        market_size_category,
        recommendation_category,
        case 
            when final_rank <= 5 and total_reviews >= 10000 then 'CORE_PORTFOLIO'
            when final_rank <= 5 and total_reviews < 1000 then 'PREMIUM_SPECIALTY'
            when total_reviews >= 10000 then 'VOLUME_PLAY'
            when total_reviews >= 5000 then 'GROWTH_OPPORTUNITY'
            else 'NICHE_OPPORTUNITY'
        end as portfolio_strategy
    from market_categories
)

select * from final_recommendations
order by final_rank 