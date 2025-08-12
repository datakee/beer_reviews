-- Provides multiple beer recommendation strategies including highest rated, 
-- balanced excellence, statistical confidence, and style diversity approaches.
{{ config(materialized='table') }}

with review_counts as (
    select
        beer_name,
        brewery_name,
        COUNT(*) as review_count,
        AVG(review_overall) as avg_overall_rating
    from {{ ref('stg_beer_reviews') }}
    group by 1, 2
),

balanced_excellence as (
    select
        be.beer_name,
        be.beer_style,
        be.brewery_name,
        be.balanced_rank as rank,
        'balanced_excellence' as ranking_method,
        rc.review_count,
        round(rc.avg_overall_rating, 3) as avg_overall_rating,
        case 
            when be.beer_name in ('Rare D.O.S.', 'Veritas 005', 'Dirty Horse') then 'Premium Portfolio'
            when be.beer_name in ('Pliny the Elder', 'Weihenstephaner Hefeweissbier', 'Two Hearted Ale') then 'Core Portfolio'
            else 'Other'
        end as portfolio_category
    from {{ ref('int_reco_balanced_excellence') }} be
    left join review_counts rc 
        on be.beer_name = rc.beer_name 
        and be.brewery_name = rc.brewery_name
    where be.balanced_rank in (1,2,3,4,5)
),

high_overall_rating as (
    select
        ho.beer_name,
        ho.beer_style,
        ho.brewery_name,
        ho.overall_rank as rank,
        'highest_overall' as ranking_method,
        rc.review_count,
        round(rc.avg_overall_rating, 3) as avg_overall_rating,
        case 
            when ho.beer_name in ('Rare D.O.S.', 'Veritas 005', 'Dirty Horse') then 'Premium Portfolio'
            when ho.beer_name in ('Pliny the Elder', 'Weihenstephaner Hefeweissbier', 'Two Hearted Ale') then 'Core Portfolio'
            else 'Other'
        end as portfolio_category
    from {{ ref('int_reco_highest_overall') }} ho
    left join review_counts rc 
        on ho.beer_name = rc.beer_name 
        and ho.brewery_name = rc.brewery_name
    where ho.overall_rank in (1,2,3,4,5)
),

diversity_ranking as (
    select
        dr.beer_name,
        dr.beer_style,
        dr.brewery_name,
        dr.diversity_rank as rank,
        'style_diversity' as ranking_method,
        rc.review_count,
        round(rc.avg_overall_rating, 3) as avg_overall_rating,
        case 
            when dr.beer_name in ('Rare D.O.S.', 'Veritas 005', 'Dirty Horse') then 'Premium Portfolio'
            when dr.beer_name in ('Pliny the Elder', 'Weihenstephaner Hefeweissbier', 'Two Hearted Ale') then 'Core Portfolio'
            else 'Other'
        end as portfolio_category
    from {{ ref('int_reco_style_diversity') }} dr
    left join review_counts rc 
        on dr.beer_name = rc.beer_name 
        and dr.brewery_name = rc.brewery_name
    where dr.diversity_rank in (1,2,3,4,5)
),

statistical_confidence_ranking as (
    select
        sc.beer_name,
        sc.beer_style,
        sc.brewery_name,
        sc.confidence_rank as rank,
        'statistical_confidence' as ranking_method,
        rc.review_count,
        round(rc.avg_overall_rating, 3) as avg_overall_rating,
        case 
            when sc.beer_name in ('Rare D.O.S.', 'Veritas 005', 'Dirty Horse') then 'Premium Portfolio'
            when sc.beer_name in ('Pliny The Elder', 'Weihenstephaner Hefeweissbier', 'Two Hearted Ale') then 'Core Portfolio'
            else 'Other'
        end as portfolio_category
    from {{ ref('int_reco_statistical_confidence') }} sc
    left join review_counts rc 
        on sc.beer_name = rc.beer_name 
        and sc.brewery_name = rc.brewery_name
    where sc.confidence_rank in (1,2,3,4,5)
)

select * from balanced_excellence
union all
select * from high_overall_rating
union all
select * from diversity_ranking
union all
select * from statistical_confidence_ranking
order by ranking_method, rank

