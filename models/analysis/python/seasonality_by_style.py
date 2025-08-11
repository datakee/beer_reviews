def model(dbt, session):
    """
    Time Series Decomposition by Beer Style - Imperial IPA vs Imperial Stout
    
    Uses moving average decomposition to separate trend and seasonal components
    Same method as seasonality_decomposition.py but applied to each beer style
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    # Load data
    df = dbt.ref("stg_beer_reviews").to_pandas()
    
    # Convert review_time to datetime
    df['review_datetime'] = pd.to_datetime(df['REVIEW_TIME'], unit='s')
    df['year_month'] = df['review_datetime'].dt.to_period('M')
    
    # Focus on two strategic styles
    target_styles = [
        'American Double / Imperial IPA',
        'American Double / Imperial Stout'
    ]
    
    df_target = df[df['BEER_STYLE'].isin(target_styles)].copy()
    
    # Results container
    all_results = []
    
    # Process each beer style separately
    for style in target_styles:
        style_df = df_target[df_target['BEER_STYLE'] == style]
        
        # Aggregate monthly ratings for this style
        monthly_ratings = style_df.groupby('year_month').agg({
            'REVIEW_OVERALL': ['mean', 'count']
        }).reset_index()
        
        # Flatten column names
        monthly_ratings.columns = ['year_month', 'avg_overall', 'total_reviews']
        
        # Convert period to timestamp for analysis
        monthly_ratings['date'] = monthly_ratings['year_month'].dt.to_timestamp()
        monthly_ratings = monthly_ratings.set_index('date').sort_index()
        
        # Filter for sufficient data
        min_reviews_threshold = 50
        monthly_ratings = monthly_ratings[monthly_ratings['total_reviews'] >= min_reviews_threshold]
        
        # Moving average decomposition (same as seasonality_decomposition.py)
        if len(monthly_ratings) >= 24:  # Need at least 2 years
            # Calculate 12-month centered moving average as trend
            trend = monthly_ratings['avg_overall'].rolling(window=12, center=True).mean()
            
            # Fill NaN values at edges
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
            
            # Add seasonal pattern summary (average by month for this style)
            mean_seasonal = seasonal.mean()
            for month_num in range(1, 13):
                month_name = ['Jan','Feb','Mar','Apr','May','Jun',
                             'Jul','Aug','Sep','Oct','Nov','Dec'][month_num-1]
                
                seasonal_component = seasonal_averages.get(month_num, 0)
                
                all_results.append({
                    'beer_style': style,
                    'month': month_num,
                    'month_name': month_name,
                    'seasonal_component': seasonal_component,
                    'seasonal_index': seasonal_component - mean_seasonal,
                    'chart_type': 'seasonal_pattern',
                    'analysis_date': datetime.now()
                })
        else:
            # Fallback for insufficient data
            for month_num in range(1, 13):
                month_name = ['Jan','Feb','Mar','Apr','May','Jun',
                             'Jul','Aug','Sep','Oct','Nov','Dec'][month_num-1]
                
                all_results.append({
                    'beer_style': style,
                    'month': month_num,
                    'month_name': month_name,
                    'seasonal_component': 0,
                    'seasonal_index': 0,
                    'chart_type': 'insufficient_data',
                    'analysis_date': datetime.now()
                })
    
    return pd.DataFrame(all_results) 