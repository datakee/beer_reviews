"""
Standalone F-test for Beer Seasonality Analysis
Connects directly to Snowflake and performs statistical significance testing
"""

import pandas as pd
import numpy as np
from scipy import stats
import snowflake.connector
from sqlalchemy import create_engine

# Snowflake connection parameters (same as your dbt profiles.yml)
SNOWFLAKE_CONFIG = {
    'account': 'PPBJHRI-FD14477',
    'user': 'KEELAN',
    'password': 'WeC3LjgJwzBSQKZ',
    'database': 'BEER_REVIEWS',
    'schema': 'PREP',
    'warehouse': 'COMPUTE_WH',
    'role': 'ACCOUNTADMIN'
}

def connect_to_snowflake():
    """Create Snowflake connection"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("✅ Connected to Snowflake successfully!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {e}")
        return None

def get_seasonality_data(conn):
    """Get seasonality results from your dbt models"""
    query = """
    SELECT 
        "MONTH",
        "MONTH_NAME", 
        "SEASONAL_INDEX",
        "CHART_TYPE"
    FROM ANALYTICS.SEASONALITY_RESULTS
    WHERE "CHART_TYPE" = 'seasonal_pattern'
    ORDER BY "MONTH"
    """
    
    try:
        df = pd.read_sql(query, conn)
        print(f"✅ Retrieved {len(df)} months of seasonal data")
        return df
    except Exception as e:
        print(f"❌ Failed to get seasonality data: {e}")
        return None

def get_monthly_raw_data(conn):
    """Get raw monthly data for F-test"""
    query = """
    WITH monthly_data AS (
        SELECT 
            EXTRACT(MONTH FROM TO_TIMESTAMP("REVIEW_TIME")) as month,
            "REVIEW_OVERALL"
        FROM PREP.STG_BEER_REVIEWS
        WHERE "REVIEW_OVERALL" IS NOT NULL
    )
    SELECT month, "REVIEW_OVERALL" as rating
    FROM monthly_data
    """
    
    try:
        df = pd.read_sql(query, conn)
        print(f"✅ Retrieved {len(df)} individual ratings for F-test")
        return df
    except Exception as e:
        print(f"❌ Failed to get raw data: {e}")
        return None

def perform_f_test(raw_data):
    """Perform F-test (ANOVA) on monthly ratings"""
    try:
        # Group ratings by month - use uppercase column names
        month_groups = [group['RATING'].values for name, group in raw_data.groupby('MONTH')]
        
        # Perform F-test
        f_statistic, p_value = stats.f_oneway(*month_groups)
        
        # Interpretation
        is_significant = p_value < 0.05
        significance_level = "SIGNIFICANT" if is_significant else "NOT SIGNIFICANT"
        
        print("\n" + "="*50)
        print("📊 F-TEST RESULTS FOR SEASONALITY")
        print("="*50)
        print(f"F-statistic: {f_statistic:.4f}")
        print(f"P-value: {p_value:.6f}")
        print(f"Significance (α=0.05): {significance_level}")
        print(f"Interpretation: {'✅ Seasonality is statistically significant' if is_significant else '❌ No significant seasonality detected'}")
        print("="*50)
        
        return {
            'f_statistic': f_statistic,
            'p_value': p_value,
            'is_significant': is_significant,
            'significance_level': significance_level
        }
        
    except Exception as e:
        print(f"❌ F-test failed: {e}")
        return None

def main():
    """Main execution function"""
    print("🚀 Starting local F-test analysis...")
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        # Get seasonality results
        seasonality_df = get_seasonality_data(conn)
        if seasonality_df is not None:
            print("\n📈 Current Seasonality Pattern:")
            print(seasonality_df[['MONTH_NAME', 'SEASONAL_INDEX']])
        
        # Get raw data for F-test
        raw_data = get_monthly_raw_data(conn)
        if raw_data is not None:
            # Perform F-test
            f_test_results = perform_f_test(raw_data)
            
            if f_test_results:
                print(f"\n💡 Business Insight:")
                if f_test_results['is_significant']:
                    print("Your seasonality analysis is statistically valid!")
                    print("You can confidently use these patterns for portfolio planning.")
                else:
                    print("The seasonal patterns may not be statistically reliable.")
                    print("Consider gathering more data or using caution in business decisions.")
    
    finally:
        conn.close()
        print("\n🔌 Disconnected from Snowflake")

if __name__ == "__main__":
    main() 