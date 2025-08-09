{{ config(materialized='table') }}

with feature_results as (
    select 
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
),

model_summary as (
    select 
        "factor",
        "raw_coefficient",
        "standardized_coefficient",
        "importance_percentage",
        "variance_explained",
        "standard_error",
        "t_statistic",
        "correlation",
        "sample_size"
    from {{ ref('feature_importance_regular_beers') }}
    where "factor" = 'MODEL_SUMMARY'
)

select * from feature_results
union all
select *, null as "rank" from model_summary
order by "rank" nulls last, "importance_percentage" desc 