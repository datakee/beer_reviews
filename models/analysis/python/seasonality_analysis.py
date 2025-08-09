import pandas as pd

def model(dbt, session):
    """
    Simple seasonality analysis: Calculate seasonal indices for visualization
    """
    
    # Load aggregated monthly data
    df = dbt.ref("seasonality_simple").to_pandas()
    
    results = []
    
    # 1. Overall seasonality (all beer styles combined)
    overall_monthly = df.groupby('MONTH')['AVG_OVERALL_RATING'].mean()
    overall_mean = overall_monthly.mean()
    
    for month in range(1, 13):
        if month in overall_monthly.index:
            seasonal_index = overall_monthly[month] / overall_mean
            avg_rating = overall_monthly[month]
        else:
            seasonal_index = 1.0
            avg_rating = overall_mean
            
        results.append({
            'month': month,
            'beer_style': None,
            'seasonal_index': seasonal_index,
            'avg_rating': avg_rating,
            'analysis_type': 'overall'
        })
    
    # 2. By beer style seasonality
    for beer_style in df['BEER_STYLE'].unique():
        style_data = df[df['BEER_STYLE'] == beer_style]
        style_monthly = style_data.groupby('MONTH')['AVG_OVERALL_RATING'].mean()
        style_mean = style_monthly.mean()
        
        for month in range(1, 13):
            if month in style_monthly.index:
                seasonal_index = style_monthly[month] / style_mean
                avg_rating = style_monthly[month]
            else:
                seasonal_index = 1.0
                avg_rating = style_mean
                
            results.append({
                'month': month,
                'beer_style': beer_style,
                'seasonal_index': seasonal_index,
                'avg_rating': avg_rating,
                'analysis_type': 'by_style'
            })
    
    return pd.DataFrame(results) 