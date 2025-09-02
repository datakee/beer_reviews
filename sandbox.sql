{{ config(
    materialized='view',
    schema='sandbox',
    tags=['sandbox', 'testing'],
    description='Sandbox model for testing and development',
    enabled=true,
    persist_docs={'relation': true, 'columns': true},
    post_hook=[
        "GRANT SELECT ON {{ this }} TO ROLE READER"
    ]
) }}

-- min and max year
select max(date_part(year, TO_TIMESTAMP(review_time))) from {{ ref('stg_beer_reviews') }}

-- model summary
select * from {{ ref('3_feature_importance_regular_beers_results') }}
