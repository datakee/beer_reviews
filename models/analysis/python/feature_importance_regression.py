import pandas as pd
import numpy as np

## QUESTION 3 ANALYSIS: Feature Importance Regression

def model(dbt, session):
    """
    Multiple Linear Regression: Which factors are most important for overall beer quality?
    
    Regression: overall_rating = β₀ + β₁(aroma) + β₂(taste) + β₃(appearance) + β₄(palate) + ε
    
    KEY INSIGHTS:
    - Uses Ordinary Least Squares (OLS) regression to find optimal coefficients
    - Standardizes coefficients to make them comparable across different rating scales
    - Calculates statistical significance (t-statistics) to validate findings
    - R-squared shows how much variance in ratings the model explains
    """
    
    # Load data and prepare for regression
    # This pulls from our intermediate table that has clean, aggregated data
    df = dbt.ref("feature_importance_analysis").to_pandas()
    feature_cols = ['REVIEW_AROMA', 'REVIEW_TASTE', 'REVIEW_APPEARANCE', 'REVIEW_PALATE']
    target_col = 'REVIEW_OVERALL'
    
    # Remove any rows with missing data to ensure clean regression
    clean_df = df.dropna(subset=feature_cols + [target_col])
    
    # Prepare matrices for regression
    # X = feature matrix (aroma, taste, appearance, palate ratings)
    # y = target variable (overall rating)
    X = clean_df[feature_cols].values
    y = clean_df[target_col].values
    # Add intercept column (β₀) - this allows the model to have a baseline rating
    X_with_intercept = np.column_stack([np.ones(X.shape[0]), X])
    
    print(f"Analyzing {len(y):,} beer reviews")
    
    # Calculate regression coefficients: β = (X'X)⁻¹X'y
    # This solves the normal equation to find optimal coefficients that minimize prediction error
    coefficients = np.linalg.lstsq(X_with_intercept, y, rcond=None)[0]
    intercept = coefficients[0]  # β₀ - baseline rating when all features = 0
    feature_coefficients = coefficients[1:]  # β₁, β₂, β₃, β₄ - feature importance weights
    
    # Model performance - R-squared calculation
    # R-squared = 1 - (SS_residual / SS_total) = proportion of variance explained by model
    y_predicted = np.dot(X_with_intercept, coefficients)
    residuals = y - y_predicted  # Prediction errors
    ss_total = np.sum((y - np.mean(y)) ** 2)  # Total variance in ratings
    ss_residual = np.sum(residuals ** 2)  # Unexplained variance
    r_squared = 1 - (ss_residual / ss_total)
    
    print(f"Model R²: {r_squared:.3f} ({r_squared*100:.1f}% variance explained)")
    
    # Standardized coefficients for importance ranking
    # Standardize features to make coefficients comparable across different scales
    # This is crucial because aroma ratings (1-5) and taste ratings (1-5) might have different variances
    X_std = (X - np.mean(X, axis=0)) / np.std(X, axis=0)
    X_std_with_intercept = np.column_stack([np.ones(X_std.shape[0]), X_std])
    std_coefficients = np.linalg.lstsq(X_std_with_intercept, y, rcond=None)[0][1:]
    
    # Calculate importance percentages
    # Convert absolute standardized coefficients to percentage importance
    # Higher absolute coefficient = more important feature
    abs_importance = np.abs(std_coefficients)
    importance_pct = (abs_importance / np.sum(abs_importance)) * 100
    
    # Standard errors and t-statistics
    # Calculate statistical significance of each coefficient
    # t-stat = coefficient / standard_error - tests if coefficient is significantly different from 0
    n, p = len(y), len(feature_coefficients)  # n = sample size, p = number of features
    mse = ss_residual / (n - p - 1)  # Mean squared error (unbiased estimate)
    XtX_inv = np.linalg.inv(np.dot(X_with_intercept.T, X_with_intercept))
    std_errors = np.sqrt(mse * np.diag(XtX_inv))  # Standard errors of coefficients
    t_stats = coefficients / std_errors  # t-statistics for hypothesis testing
    
    # Correlations for comparison
    # Calculate simple correlations as baseline for feature importance
    # This shows the raw relationship between each feature and overall rating
    correlations = np.corrcoef(X.T, y)[-1, :-1]
    
    # Build results table with all statistical measures
    results = []
    for i, feature in enumerate(feature_cols):
        factor_name = feature.replace('REVIEW_', '').lower()
        results.append({
            'factor': factor_name,
            'raw_coefficient': feature_coefficients[i],  # Original scale coefficient
            'standardized_coefficient': std_coefficients[i],  # Comparable across features
            'importance_percentage': importance_pct[i],  # Relative importance (0-100%)
            'standard_error': std_errors[i + 1],  # Uncertainty in coefficient estimate
            't_statistic': t_stats[i + 1],  # Statistical significance (|t| > 2 = significant)
            'correlation': correlations[i],  # Simple correlation with overall rating
            'sample_size': n  # Number of observations used
        })
        
        print(f"{factor_name.upper()}: {importance_pct[i]:.1f}% importance, coef={std_coefficients[i]:.3f}")
    
    results_df = pd.DataFrame(results)
    # Rank features by importance percentage (highest = most important)
    results_df['rank'] = results_df['importance_percentage'].rank(ascending=False, method='min')
    
    # Add model summary row with overall statistics
    summary = pd.DataFrame([{
        'factor': 'MODEL_SUMMARY',
        'raw_coefficient': intercept,  # Baseline rating
        'standardized_coefficient': r_squared,  # Model fit (0-1)
        'importance_percentage': n,  # Sample size
        'standard_error': np.sqrt(mse),  # Overall model error
        't_statistic': np.mean(np.abs(t_stats[1:])),  # Average feature significance
        'correlation': np.mean(correlations),  # Average feature correlation
        'sample_size': n  # Total observations
    }])
    
    final_results = pd.concat([results_df, summary], ignore_index=True)
    
    print(f"Analysis complete! Most important factor: {results_df.loc[results_df['rank']==1, 'factor'].iloc[0].upper()}")
    
    return final_results 