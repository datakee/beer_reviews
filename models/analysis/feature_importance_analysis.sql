-- Performs multiple linear regression to determine 
-- which rating components most significantly affect overall beer ratings.
{{ config(materialized='table') }}

/*
ANALYSIS: Feature Importance for Overall Beer Quality (Simplified)

PURPOSE:
Clean, simple dataset for regression analysis to answer:
"Which factors (aroma, taste, appearance, palate) are most important 
in determining overall beer quality?"

METHODOLOGY:
- Simple multiple linear regression: overall = f(aroma, taste, appearance, palate)
- Include basic controls: brewery_name, beer_style, beer_abv
- Filter for complete records only

STATISTICAL APPROACH:
overall_rating = β₀ + β₁(aroma) + β₂(taste) + β₃(appearance) + β₄(palate) + controls + ε

The β coefficients will tell us the relative importance of each factor.
*/

with clean_data as (
    select 
        -- Dependent variable
        review_overall,
        
        -- Independent variables (our main interest)
        review_aroma,
        review_taste,
        review_appearance,
        review_palate,
        
        -- Control variables
        brewery_name,
        beer_style,
        beer_abv,
        
        -- Identifiers (for reference)
        beer_name
        
    from {{ ref('stg_beer_reviews') }}
    
    -- Filter for complete records only
    where review_overall is not null
      and review_aroma is not null
      and review_taste is not null
      and review_appearance is not null
      and review_palate is not null
      and brewery_name is not null
      and beer_style is not null
)

select * from clean_data
order by random()  -- Randomize for sampling if needed 