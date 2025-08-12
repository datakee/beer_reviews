{{ config(materialized='table') }}

-- Comprehensive analysis of rating component relationships

with component_data as (
    select
        review_overall,
        review_aroma,
        review_appearance,
        review_taste,
        review_palate,
        beer_style,
        beer_abv,
        brewery_name
    from {{ ref('stg_beer_reviews') }}
    where review_overall is not null
      and review_aroma is not null
      and review_appearance is not null
      and review_taste is not null
      and review_palate is not null
),

-- 1. Overall correlations between components
correlation_analysis as (
    select
        'aroma_vs_overall' as relationship,
        corr(review_aroma, review_overall) as correlation_coefficient,
        count(*) as sample_size
    from component_data
    
    union all
    
    select
        'appearance_vs_overall' as relationship,
        corr(review_appearance, review_overall) as correlation_coefficient,
        count(*) as sample_size
    from component_data
    
    union all
    
    select
        'taste_vs_overall' as relationship,
        corr(review_taste, review_overall) as correlation_coefficient,
        count(*) as sample_size
    from component_data
    
    union all
    
    select
        'palate_vs_overall' as relationship,
        corr(review_palate, review_overall) as correlation_coefficient,
        count(*) as sample_size
    from component_data
    
    union all
    
    select
        'aroma_vs_taste' as relationship,
        corr(review_aroma, review_taste) as correlation_coefficient,
        count(*) as sample_size
    from component_data
    
    union all
    
    select
        'appearance_vs_aroma' as relationship,
        corr(review_appearance, review_aroma) as correlation_coefficient,
        count(*) as sample_size
    from component_data
),

-- 2. Component interaction effects
interaction_effects as (
    select
        beer_style,
        count(*) as total_reviews,
        -- High aroma + high appearance combinations
        avg(case when review_aroma >= 4.0 and review_appearance >= 4.0 
            then review_overall else null end) as high_aroma_appearance_overall,
        -- High taste + high palate combinations  
        avg(case when review_taste >= 4.0 and review_palate >= 4.0 
            then review_overall else null end) as high_taste_palate_overall,
        -- Balanced high scores across all components
        avg(case when review_aroma >= 4.0 and review_appearance >= 4.0 
                   and review_taste >= 4.0 and review_palate >= 4.0
            then review_overall else null end) as balanced_high_overall
    from component_data
    where beer_style is not null
    group by beer_style
    having count(*) >= 100
),

-- 3. Component consistency analysis
consistency_analysis as (
    select
        beer_style,
        count(*) as total_reviews,
        -- Standard deviation across components (lower = more consistent)
        stddev(review_aroma) as aroma_stddev,
        stddev(review_appearance) as appearance_stddev,
        stddev(review_taste) as taste_stddev,
        stddev(review_palate) as palate_stddev,
        -- Overall consistency (average stddev across all components)
        (stddev(review_aroma) + stddev(review_appearance) + 
         stddev(review_taste) + stddev(review_palate)) / 4 as overall_consistency
    from component_data
    where beer_style is not null
    group by beer_style
    having count(*) >= 50
),

-- 4. Component strength patterns
strength_patterns as (
    select
        beer_style,
        count(*) as total_reviews,
        -- Which component is strongest for each style
        case 
            when avg(review_aroma) = greatest(avg(review_aroma), avg(review_appearance), 
                                            avg(review_taste), avg(review_palate)) then 'aroma'
            when avg(review_appearance) = greatest(avg(review_aroma), avg(review_appearance), 
                                                 avg(review_taste), avg(review_palate)) then 'appearance'
            when avg(review_taste) = greatest(avg(review_aroma), avg(review_appearance), 
                                            avg(review_taste), avg(review_palate)) then 'taste'
            else 'palate'
        end as strongest_component,
        -- Component scores
        round(avg(review_aroma), 3) as avg_aroma,
        round(avg(review_appearance), 3) as avg_appearance,
        round(avg(review_taste), 3) as avg_taste,
        round(avg(review_palate), 3) as avg_palate,
        round(avg(review_overall), 3) as avg_overall
    from component_data
    where beer_style is not null
    group by beer_style
    having count(*) >= 50
)

-- Final output combining all analyses
select 
    'correlation' as analysis_type,
    relationship as metric,
    round(correlation_coefficient, 4) as value,
    sample_size as sample_size,
    null as beer_style,
    null as strongest_component
from correlation_analysis

union all

select 
    'interaction' as analysis_type,
    'high_aroma_appearance_overall' as metric,
    round(high_aroma_appearance_overall, 3) as value,
    total_reviews as sample_size,
    beer_style,
    null as strongest_component
from interaction_effects
where high_aroma_appearance_overall is not null

union all

select 
    'interaction' as analysis_type,
    'high_taste_palate_overall' as metric,
    round(high_taste_palate_overall, 3) as value,
    total_reviews as sample_size,
    beer_style,
    null as strongest_component
from interaction_effects
where high_taste_palate_overall is not null

union all

select 
    'consistency' as analysis_type,
    'overall_consistency' as metric,
    round(overall_consistency, 3) as value,
    total_reviews as sample_size,
    beer_style,
    null as strongest_component
from consistency_analysis

union all

select 
    'strength_pattern' as analysis_type,
    'component_scores' as metric,
    avg_overall as value,
    total_reviews as sample_size,
    beer_style,
    strongest_component
from strength_patterns

order by analysis_type, metric, value desc
