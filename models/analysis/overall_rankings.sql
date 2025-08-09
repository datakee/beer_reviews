{{ config(materialized='table') }}

with balanced_excellence as (
    select
        beer_name,
        beer_style,
        brewery_name,
        balanced_rank as rank,
        'balanced_excellence' as ranking_method
    from {{ ref('int_reco_balanced_excellence') }}
    where balanced_rank in (1,2,3,4,5)
),

high_overall_rating as (
    select
        beer_name,
        beer_style,
        brewery_name,
        overall_rank as rank,
        'highest_overall' as ranking_method
    from {{ ref('int_reco_highest_overall') }}
    where overall_rank in (1,2,3,4,5)
),

diversity_ranking as (
    select
        beer_name,
        beer_style,
        brewery_name,
        diversity_rank as rank,
        'style_diversity' as ranking_method

    from {{ ref('int_reco_style_diversity') }}
    where diversity_rank in (1,2,3,4,5)
),

statistical_confidence_ranking as (
    select
        beer_name,
        beer_style,
        brewery_name,
        confidence_rank as rank,
        'statistical_confidence' as ranking_method
    from {{ ref('int_reco_statistical_confidence') }}
    where confidence_rank in (1,2,3,4,5)
)

select * from balanced_excellence
union all
select * from high_overall_rating
union all
select * from diversity_ranking
union all
select * from statistical_confidence_ranking
order by 5,4

