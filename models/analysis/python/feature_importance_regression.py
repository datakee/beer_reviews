import pandas as pd
import numpy as np

## QUESTION 3 ANALYSIS: Feature Importance Regression

def model(dbt, session):
    """
    Multiple Linear Regression: Which factors are most important for overall beer quality?
    
    Regression: overall_rating = β₀ + β₁(aroma) + β₂(taste) + β₃(appearance) + β₄(palate) + ε
    """
    
    # Load data and prepare for regression
    df = dbt.ref("feature_importance_analysis").to_pandas()
    feature_cols = ['REVIEW_AROMA', 'REVIEW_TASTE', 'REVIEW_APPEARANCE', 'REVIEW_PALATE']
    target_col = 'REVIEW_OVERALL'
    
    clean_df = df.dropna(subset=feature_cols + [target_col])
    
    # Prepare matrices for regression
    X = clean_df[feature_cols].values
    y = clean_df[target_col].values
    X_with_intercept = np.column_stack([np.ones(X.shape[0]), X])
    
    print(f"Analyzing {len(y):,} beer reviews")
    
    # Calculate regression coefficients: β = (X'X)⁻¹X'y
    coefficients = np.linalg.lstsq(X_with_intercept, y, rcond=None)[0]
    intercept = coefficients[0]
    feature_coefficients = coefficients[1:]
    
    # Model performance
    y_predicted = np.dot(X_with_intercept, coefficients)
    residuals = y - y_predicted
    ss_total = np.sum((y - np.mean(y)) ** 2)
    ss_residual = np.sum(residuals ** 2)
    r_squared = 1 - (ss_residual / ss_total)
    
    print(f"Model R²: {r_squared:.3f} ({r_squared*100:.1f}% variance explained)")
    
    # Standardized coefficients for importance ranking
    X_std = (X - np.mean(X, axis=0)) / np.std(X, axis=0)
    X_std_with_intercept = np.column_stack([np.ones(X_std.shape[0]), X_std])
    std_coefficients = np.linalg.lstsq(X_std_with_intercept, y, rcond=None)[0][1:]
    
    # Calculate importance percentages
    abs_importance = np.abs(std_coefficients)
    importance_pct = (abs_importance / np.sum(abs_importance)) * 100
    
    # Standard errors and t-statistics
    n, p = len(y), len(feature_coefficients)
    mse = ss_residual / (n - p - 1)
    XtX_inv = np.linalg.inv(np.dot(X_with_intercept.T, X_with_intercept))
    std_errors = np.sqrt(mse * np.diag(XtX_inv))
    t_stats = coefficients / std_errors
    
    # Correlations for comparison
    correlations = np.corrcoef(X.T, y)[-1, :-1]
    
    # Build results
    results = []
    for i, feature in enumerate(feature_cols):
        factor_name = feature.replace('REVIEW_', '').lower()
        results.append({
            'factor': factor_name,
            'raw_coefficient': feature_coefficients[i],
            'standardized_coefficient': std_coefficients[i],
            'importance_percentage': importance_pct[i],
            'standard_error': std_errors[i + 1],
            't_statistic': t_stats[i + 1],
            'correlation': correlations[i],
            'sample_size': n
        })
        
        print(f"{factor_name.upper()}: {importance_pct[i]:.1f}% importance, coef={std_coefficients[i]:.3f}")
    
    results_df = pd.DataFrame(results)
    results_df['rank'] = results_df['importance_percentage'].rank(ascending=False, method='min')
    
    # Add model summary
    summary = pd.DataFrame([{
        'factor': 'MODEL_SUMMARY',
        'raw_coefficient': intercept,
        'standardized_coefficient': r_squared,
        'importance_percentage': n,
        'standard_error': np.sqrt(mse),
        't_statistic': np.mean(np.abs(t_stats[1:])),
        'correlation': np.mean(correlations),
        'sample_size': n
    }])
    
    final_results = pd.concat([results_df, summary], ignore_index=True)
    
    print(f"Analysis complete! Most important factor: {results_df.loc[results_df['rank']==1, 'factor'].iloc[0].upper()}")
    
    return final_results 