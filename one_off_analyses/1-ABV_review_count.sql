/*
ONE-OFF ANALYSIS: Review Count by ABV Buckets

Counts total reviews for different ABV strength categories
*/

select 
    case 
        when beer_abv >= 13 then 'Ultra Strong (13%+)'
        when beer_abv >= 7 then 'Strong (7-12%)'
        when beer_abv < 7 then 'Regular (<7%)'
        else 'Unknown'
    end as abv_category,
    count(*) as review_count,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage_of_reviews
from {{ ref('stg_beer_reviews') }}
where beer_abv is not null
group by 1
order by 2 desc 