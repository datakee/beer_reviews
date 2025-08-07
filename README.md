# Beer Reviews Analysis - TGTG Case Study

This dbt project analyzes beer review data to answer key business questions for the Too Good To Go Senior Product Analyst case study.

## Project Structure

```
beer_analysis/
├── models/
│   ├── sources.yml           # Raw data source definitions
│   ├── staging/              # Staging layer (cleaned raw data)
│   │   └── stg_beer_reviews.sql
│   └── analysis/             # Analysis models for case study questions
│       └── brewery_strength_analysis.sql
├── analyses/                 # Ad-hoc analyses and tests
└── macros/                   # Reusable SQL macros
```

## Case Study Questions

1. **Which brewery produces the strongest beers by ABV%?**
2. **If you had to pick 3 beers to recommend using only this data, which would you pick?**
3. **Which factors (aroma, taste, appearance, palette) are most important in determining overall quality?**
4. **If I typically enjoy a beer due to its aroma and appearance, which beer style should I try?**
5. **Use any method to investigate seasonality in overall ratings.**

## Setup

1. Ensure you have dbt-snowflake installed
2. Configure your `profiles.yml` with Snowflake connection details
3. Run `dbt run` to build all models
4. Use `dbt test` to validate data quality

## Data Source

- **Database**: BEER_REVIEWS_RAW
- **Schema**: PUBLIC  
- **Table**: beer_reviews_raw
- **Source**: Beer review dataset with ratings, ABV, brewery info, and timestamps
