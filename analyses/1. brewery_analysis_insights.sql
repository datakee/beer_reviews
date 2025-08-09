-- Analysis to understand what the strongest brewery data actually shows

with top_5_breweries as (
    select brewery_name
    from {{ ref('brewery_strength_analysis') }}
    where rank_by_avg_abv <= 5
),

brewery_styles as (
    select 
        s.brewery_name,
        s.beer_style,
        COUNT(*) as beer_count_in_style,
        AVG(s.review_overall) as avg_rating_in_style,
        ROW_NUMBER() over (partition by s.brewery_name order by COUNT(*) desc) as style_rank
    from {{ ref('stg_beer_reviews') }} s
    inner join top_5_breweries t on s.brewery_name = t.brewery_name
    where s.beer_style is not null
    group by 1, 2
),

final_results as (
    select 
        b.brewery_name,
        ba.avg_abv,
        ba.total_beers,
        ba.rank_by_avg_abv,
        bs.beer_style,
        bs.beer_count_in_style,
        bs.avg_rating_in_style,
        bs.style_rank
    from brewery_styles bs
    inner join {{ ref('brewery_strength_analysis') }} ba 
        on bs.brewery_name = ba.brewery_name
    inner join top_5_breweries b 
        on bs.brewery_name = b.brewery_name
    where bs.style_rank <= 3
)

select * from final_results
order by rank_by_avg_abv, style_rank 