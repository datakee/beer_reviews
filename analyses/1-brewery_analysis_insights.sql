-- All beers from the strongest brewery (rank 1)

with strongest_brewery as (
    select brewery_name
    from {{ ref('brewery_strength_analysis') }}
    where rank_by_avg_abv = 1
    
),

brewery_beers as (
    select 
        s.brewery_name,
        s.beer_name,
        s.beer_style,
        s.beer_abv,
        AVG(s.review_overall) as avg_overall_rating,
        AVG(s.review_aroma) as avg_aroma,
        AVG(s.review_taste) as avg_taste,
        AVG(s.review_appearance) as avg_appearance,
        AVG(s.review_palate) as avg_palate,
        COUNT(*) as review_count
    from {{ ref('stg_beer_reviews') }} s
    inner join strongest_brewery sb on s.brewery_name = sb.brewery_name
    where s.beer_name is not null
      and s.beer_abv is not null
    group by 1, 2, 3, 4
)

select * from brewery_beers
order by beer_abv desc, avg_overall_rating desc 