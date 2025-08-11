/*
ONE-OFF ANALYSIS: Top Individual Beers in Best Aroma/Appearance Styles

BUSINESS QUESTION:
Within the top 2 beer styles for aroma + appearance, which specific beers rank highest?

PURPOSE:
Identify specific beer recommendations within our strategic style categories
*/

with top_styles as (
    -- Get the top 2 beer styles by combined aroma + appearance score
    select 
        beer_style,
        final_recommendation_rank
    from {{ ref('aroma_appearance_recommendations') }}
    --where final_recommendation_rank <= 3
    where beer_style = 'American Double / Imperial IPA'
),

beer_performance as (
    -- Get individual beer performance within these top styles
    select 
        b.beer_name,
        b.brewery_name,
        b.beer_style,
        count(*) as total_reviews,
        avg(b.review_aroma) as avg_aroma,
        avg(b.review_appearance) as avg_appearance,
        (avg(b.review_aroma) + avg(b.review_appearance)) / 2 as combined_aroma_appearance_score,
        avg(b.review_overall) as avg_overall,
        avg(b.beer_abv) as avg_abv
    from {{ ref('stg_beer_reviews') }} b
    inner join top_styles ts on b.beer_style = ts.beer_style
    where b.review_aroma is not null
      and b.review_appearance is not null
      and b.beer_name is not null
      and b.brewery_name is not null
    group by b.beer_name, b.brewery_name, b.beer_style
    having count(*) >= 10  -- Minimum reviews for reliability
),

ranked_beers as (
    select 
        *,
        row_number() over (
            partition by beer_style 
            order by combined_aroma_appearance_score desc, total_reviews desc
        ) as rank_within_style,
        row_number() over (
            order by combined_aroma_appearance_score desc, total_reviews desc
        ) as overall_rank
    from beer_performance
),

final as (select
    beer_style,
    rank_within_style,
    overall_rank,
    beer_name,
    brewery_name,
    total_reviews,
    round(avg_aroma, 3) as avg_aroma,
    round(avg_appearance, 3) as avg_appearance,
    round(combined_aroma_appearance_score, 3) as combined_score,
    round(avg_overall, 3) as avg_overall,
    round(avg_abv, 2) as avg_abv_percent,
    case 
        when rank_within_style = 1 then 'TOP_PICK'
        when rank_within_style <= 3 then 'RECOMMENDED'
        else 'CONSIDER'
    end as recommendation_tier
from ranked_beers
where rank_within_style <= 5  -- Top 5 beers per style
order by beer_style, rank_within_style)

select * from final