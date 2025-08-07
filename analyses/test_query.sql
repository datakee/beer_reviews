-- Test query for dbt Power User
SELECT * FROM {{ ref('stg_beer_reviews') }} LIMIT 10 