{{ config(materialized='table') }}

with top_1_results as (
    select 
        'Premium (Top 1 Style)' as market_segment,
        "factor",
        "raw_coefficient",
        "standardized_coefficient",
        "importance_percentage",
        "variance_explained",
        "standard_error",
        "t_statistic",
        "correlation",
        "sample_size",
        "rank"
    from {{ ref('feature_importance_regression_top_1') }}
    where "factor" != 'MODEL_SUMMARY'
),

strong_beers_results as (
    select 
        'Specialty (Strong Beers >10% ABV)' as market_segment,
        "factor",
        "raw_coefficient",
        "standardized_coefficient",
        "importance_percentage",
        "variance_explained",
        "standard_error",
        "t_statistic",
        "correlation",
        "sample_size",
        "rank"
    from {{ ref('feature_importance_strong_beers') }}
    where "factor" != 'MODEL_SUMMARY'
),

regular_beers_results as (
    select 
        'Mainstream (Regular Beers)' as market_segment,
        "factor",
        "raw_coefficient",
        "standardized_coefficient",
        "importance_percentage",
        "variance_explained",
        "standard_error",
        "t_statistic",
        "correlation",
        "sample_size",
        "rank"
    from {{ ref('feature_importance_regular_beers') }}
    where "factor" != 'MODEL_SUMMARY'
)

select * from top_1_results
union all
select * from strong_beers_results
union all
select * from regular_beers_results
order by market_segment, "rank" 