def model(dbt, session):
    """
    Time Series Decomposition for Beer Ratings Seasonality
    
    Uses moving average decomposition to separate:
    - Trend (12-month moving average)
    - Seasonal (repeating patterns after trend removal)
    - Residual (noise)
    
    KEY INSIGHTS:
    - Centered 12-month moving average reveals long-term rating trends
    - Detrending isolates seasonal patterns from overall rating changes
    - Seasonal indices show which months consistently have higher/lower ratings
    - Seasonal strength measures how pronounced seasonal patterns are
    
    Returns clean seasonal component for visualization
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    # Load data from staging - raw beer review data with timestamps
    df = dbt.ref("stg_beer_reviews").to_pandas()
    
    # Convert review_time to datetime and extract date components (use uppercase column names)
    # Unix timestamp conversion and monthly period extraction for time series analysis
    df['review_datetime'] = pd.to_datetime(df['REVIEW_TIME'], unit='s')
    df['year_month'] = df['review_datetime'].dt.to_period('M')
    
    # Aggregate monthly ratings
    # Group by month to create time series of average ratings and review counts
    monthly_ratings = df.groupby('year_month').agg({
        'REVIEW_OVERALL': ['mean', 'count'],  # Overall rating average + count per month
        'REVIEW_AROMA': 'mean',               # Monthly aroma rating averages
        'REVIEW_APPEARANCE': 'mean',          # Monthly appearance rating averages
        'REVIEW_TASTE': 'mean',               # Monthly taste rating averages
        'REVIEW_PALATE': 'mean'               # Monthly palate rating averages
    }).reset_index()
    
    # Flatten column names for easier analysis
    monthly_ratings.columns = [
        'year_month', 'avg_overall', 'total_reviews', 
        'avg_aroma', 'avg_appearance', 'avg_taste', 'avg_palate'
    ]
    
    # Convert period to timestamp for analysis
    # Set date as index for time series operations
    monthly_ratings['date'] = monthly_ratings['year_month'].dt.to_timestamp()
    monthly_ratings = monthly_ratings.set_index('date').sort_index()
    
    # Filter for sufficient data (need at least 2 full years for reliable seasonality)
    # Small sample months could create misleading seasonal patterns
    min_reviews_threshold = 100
    monthly_ratings = monthly_ratings[monthly_ratings['total_reviews'] >= min_reviews_threshold]
    
    # Simple moving average decomposition
    if len(monthly_ratings) >= 24:  # Need at least 2 years for reliable seasonality
        # Calculate 12-month centered moving average as trend
        # CENTERED = 6 months before + current + 6 months after (not trailing)
        # This provides unbiased trend estimates without lag
        trend = monthly_ratings['avg_overall'].rolling(window=12, center=True).mean()
        
        # Fill NaN values at edges with linear interpolation
        # Edge months don't have full 12-month windows, so we interpolate
        trend = trend.interpolate(method='linear')
        trend = trend.fillna(method='bfill').fillna(method='ffill')
        
        # Calculate detrended series
        # Detrended = Original - Trend = removes long-term changes to isolate seasonal patterns
        detrended = monthly_ratings['avg_overall'] - trend
        
        # Calculate seasonal component (average by month after detrending)
        # Group detrended data by month to find consistent seasonal patterns
        monthly_data = pd.DataFrame({
            'month': monthly_ratings.index.month,
            'detrended': detrended
        })
        
        # Average detrended values for each month across all years
        # This reveals which months consistently have higher/lower ratings
        seasonal_averages = monthly_data.groupby('month')['detrended'].mean()
        
        # Map seasonal averages back to full time series
        # Each month gets its historical seasonal average
        seasonal = monthly_ratings.index.to_series().dt.month.map(seasonal_averages)
        
        # Calculate residual (noise)
        # Residual = Detrended - Seasonal = random variation not explained by trend or seasonality
        residual = detrended - seasonal
        
        # Create detailed results table with all decomposition components
        decomposition_results = pd.DataFrame({
            'date': monthly_ratings.index,
            'original_rating': monthly_ratings['avg_overall'],      # Raw monthly averages
            'trend_component': trend,                              # Long-term trend (12-month MA)
            'seasonal_component': seasonal,                        # Repeating monthly patterns
            'residual_component': residual,                        # Unexplained noise
            'detrended_rating': detrended,                         # Trend-removed data
            'total_reviews': monthly_ratings['total_reviews'],     # Sample size per month
            'year': monthly_ratings.index.year,                    # Year for grouping
            'month': monthly_ratings.index.month,                  # Month (1-12)
            'month_name': monthly_ratings.index.strftime('%B')     # Month name for display
        })
        
        # Calculate seasonal metrics for interpretation
        mean_seasonal = seasonal.mean()  # Average seasonal effect across all months
        decomposition_results['seasonal_index'] = seasonal - mean_seasonal  # Deviation from average
        # Seasonal strength = how much seasonal variation exists relative to total variation
        decomposition_results['seasonal_strength'] = abs(seasonal.std()) / abs(monthly_ratings['avg_overall'].std())
        decomposition_results['chart_type'] = 'monthly_decomposition'
        decomposition_results['analysis_date'] = datetime.now()
        
        # Create clean seasonal pattern summary (average by month)
        # This shows the consistent seasonal effect for each month across all years
        seasonal_pattern = []
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        for month_num in range(1, 13):
            seasonal_pattern.append({
                'month': month_num,
                'month_name': month_names[month_num-1],
                'seasonal_component': seasonal_averages.get(month_num, 0),  # Raw seasonal effect
                'seasonal_index': seasonal_averages.get(month_num, 0) - mean_seasonal,  # Relative to average
                'chart_type': 'seasonal_pattern',
                'analysis_date': datetime.now(),
                'total_reviews': monthly_data[monthly_data['month'] == month_num].shape[0] * 1000  # Approximate
            })
        
        seasonal_summary = pd.DataFrame(seasonal_pattern)
        
        # Combine detailed decomposition with seasonal summary
        final_results = pd.concat([
            decomposition_results.reset_index(drop=True),
            seasonal_summary
        ], ignore_index=True)
        
        # Add interpretation columns for easier analysis
        # Shows which months are above/below the average seasonal effect
        final_results['trend_direction'] = np.where(
            final_results['seasonal_component'] > mean_seasonal, 'ABOVE_AVERAGE', 'BELOW_AVERAGE'
        )
        
    else:
        # Fallback for insufficient data
        # Need at least 24 months (2 years) for reliable seasonality detection
        final_results = pd.DataFrame({
            'date': [datetime.now()],
            'chart_type': ['insufficient_data'],
            'seasonal_component': [0],
            'seasonal_index': [0],
            'analysis_date': [datetime.now()],
            'error_message': ['Insufficient data for reliable seasonality analysis - need at least 24 months']
        })
    
    return final_results 