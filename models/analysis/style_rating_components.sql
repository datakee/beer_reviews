-- Analyzes the gap between aroma/appearance and taste scores to identify which beer styles exceed vs disappoint customer expectations.
{{ config(materialized='table') }}

with style_components as (
    select
        beer_style,
        count(*) as total_reviews,
        avg(review_aroma) as avg_aroma,
        avg(review_appearance) as avg_appearance,
        avg(review_taste) as avg_taste,
        avg(review_overall) as avg_overall
    from {{ ref('stg_beer_reviews') }}
    where beer_style is not null
      and review_aroma is not null
      and review_appearance is not null
      and review_taste is not null
    group by beer_style
    having count(*) >= 50  -- Minimum reviews for reliability
),

gap_analysis as (
    select
        *,
        -- Simple gap: aroma/appearance vs taste
        (avg_aroma + avg_appearance) / 2 as avg_aroma_appearance,
        (avg_aroma + avg_appearance) / 2 - avg_taste as appearance_taste_gap
    from style_components
),

ranked_styles as (
    select
        *,
        -- Simple gap classification
        case 
            when appearance_taste_gap >= 0.1 then 'LOOKS_BETTER_THAN_TASTES'
            when appearance_taste_gap <= -0.1 then 'TASTES_BETTER_THAN_LOOKS'
            else 'BALANCED'
        end as gap_type
    from gap_analysis
)

select
    beer_style,
    total_reviews,
    round(appearance_taste_gap, 3) as appearance_taste_gap,
    gap_type

from ranked_styles
where beer_style like '%Imperial%' or beer_style like '%Low%'
order by appearance_taste_gap desc 
