def model(dbt, session):
    """
    Time Series Decomposition for Beer Ratings Seasonality
    
    Uses moving average decomposition to separate:
    - Trend (12-month moving average)
    - Seasonal (repeating patterns after trend removal)
    - Residual (noise)
    
    Returns clean seasonal component for visualization
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    # Load data from staging
    df = dbt.ref("stg_beer_reviews").to_pandas()
    
    # Convert review_time to datetime and extract date components (use uppercase column names)
    df['review_datetime'] = pd.to_datetime(df['REVIEW_TIME'], unit='s')
    df['year_month'] = df['review_datetime'].dt.to_period('M')
    
    # Aggregate monthly ratings
    monthly_ratings = df.groupby('year_month').agg({
        'REVIEW_OVERALL': ['mean', 'count'],
        'REVIEW_AROMA': 'mean',
        'REVIEW_APPEARANCE': 'mean',
        'REVIEW_TASTE': 'mean',
        'REVIEW_PALATE': 'mean'
    }).reset_index()
    
    # Flatten column names
    monthly_ratings.columns = [
        'year_month', 'avg_overall', 'total_reviews', 
        'avg_aroma', 'avg_appearance', 'avg_taste', 'avg_palate'
    ]
    
    # Convert period to timestamp for analysis
    monthly_ratings['date'] = monthly_ratings['year_month'].dt.to_timestamp()
    monthly_ratings = monthly_ratings.set_index('date').sort_index()
    
    # Filter for sufficient data (need at least 2 full years for reliable seasonality)
    min_reviews_threshold = 100
    monthly_ratings = monthly_ratings[monthly_ratings['total_reviews'] >= min_reviews_threshold]
    
    # Simple moving average decomposition
    if len(monthly_ratings) >= 24:  # Need at least 2 years
        # Calculate 12-month centered moving average as trend
        trend = monthly_ratings['avg_overall'].rolling(window=12, center=True).mean()
        
        # Fill NaN values at edges with linear interpolation
        trend = trend.interpolate(method='linear')
        trend = trend.fillna(method='bfill').fillna(method='ffill')
        
        # Calculate detrended series
        detrended = monthly_ratings['avg_overall'] - trend
        
        # Calculate seasonal component (average by month after detrending)
        monthly_data = pd.DataFrame({
            'month': monthly_ratings.index.month,
            'detrended': detrended
        })
        
        seasonal_averages = monthly_data.groupby('month')['detrended'].mean()
        
        # Map seasonal averages back to full time series
        seasonal = monthly_ratings.index.to_series().dt.month.map(seasonal_averages)
        
        # Calculate residual
        residual = detrended - seasonal
        
        # Create detailed results
        decomposition_results = pd.DataFrame({
            'date': monthly_ratings.index,
            'original_rating': monthly_ratings['avg_overall'],
            'trend_component': trend,
            'seasonal_component': seasonal,
            'residual_component': residual,
            'detrended_rating': detrended,
            'total_reviews': monthly_ratings['total_reviews'],
            'year': monthly_ratings.index.year,
            'month': monthly_ratings.index.month,
            'month_name': monthly_ratings.index.strftime('%B')
        })
        
        # Calculate seasonal metrics
        mean_seasonal = seasonal.mean()
        decomposition_results['seasonal_index'] = seasonal - mean_seasonal
        decomposition_results['seasonal_strength'] = abs(seasonal.std()) / abs(monthly_ratings['avg_overall'].std())
        decomposition_results['chart_type'] = 'monthly_decomposition'
        decomposition_results['analysis_date'] = datetime.now()
        
        # Create clean seasonal pattern summary (average by month)
        seasonal_pattern = []
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        for month_num in range(1, 13):
            seasonal_pattern.append({
                'month': month_num,
                'month_name': month_names[month_num-1],
                'seasonal_component': seasonal_averages.get(month_num, 0),
                'seasonal_index': seasonal_averages.get(month_num, 0) - mean_seasonal,
                'chart_type': 'seasonal_pattern',
                'analysis_date': datetime.now(),
                'total_reviews': monthly_data[monthly_data['month'] == month_num].shape[0] * 1000  # Approximate
            })
        
        seasonal_summary = pd.DataFrame(seasonal_pattern)
        
        # Combine results
        final_results = pd.concat([
            decomposition_results.reset_index(drop=True),
            seasonal_summary
        ], ignore_index=True)
        
        # Add interpretation columns
        final_results['trend_direction'] = np.where(
            final_results['seasonal_component'] > mean_seasonal, 'ABOVE_AVERAGE', 'BELOW_AVERAGE'
        )
        
    else:
        # Fallback for insufficient data
        final_results = pd.DataFrame({
            'date': [datetime.now()],
            'chart_type': ['insufficient_data'],
            'seasonal_component': [0],
            'seasonal_index': [0],
            'analysis_date': [datetime.now()],
            'error_message': ['Insufficient data for reliable seasonality analysis - need at least 24 months']
        })
    
    return final_results 