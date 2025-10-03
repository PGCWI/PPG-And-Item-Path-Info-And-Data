# services/analysis.py
import pandas as pd
import numpy as np

def analyze_incorrect_counts(df, threshold=2):
    """
    Analyzes count transactions to identify incorrect counts
    
    Args:
        df: DataFrame containing count transactions
        threshold: The threshold for acceptable count variance (default: 2)
        
    Returns:
        Dictionary with analysis results
    """
    # Ensure DataFrame is copied and types are properly inferred
    counts_df = df.copy().infer_objects()
    
    # Convert creation date to datetime for proper sorting/comparison
    counts_df['creationDate'] = pd.to_datetime(counts_df['creationDate'], format='ISO8601')
    
    # Sort by location, material, and creation date
    counts_df = counts_df.sort_values(['locationName', 'materialName', 'qualification', 'creationDate'])
    
    # Add a column for count variance - absolute difference between requested and confirmed
    counts_df['variance'] = counts_df['quantityExpected'] - counts_df['quantityConfirmed']
    counts_df['absoluteVariance'] = np.abs(counts_df['variance'])
    
    # Identify if count is within threshold
    counts_df['withinThreshold'] = counts_df['absoluteVariance'] <= threshold
    
    # Group by location to count how many times each location was counted
    location_counts = counts_df.groupby(['locationName', 'materialName', 'qualification']).size().reset_index(name='timesLocationCounted')
    
    # Merge this information back to the main dataframe
    counts_df = pd.merge(counts_df, location_counts, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Only consider locations that were counted multiple times
    multi_count_df = counts_df[counts_df['timesLocationCounted'] > 1].copy()
    
    # Identify the first count for each location/material combination
    multi_count_df['isFirstCount'] = False
    first_counts = multi_count_df.groupby(['locationName', 'materialName', 'qualification'])['creationDate'].transform('min')
    multi_count_df.loc[multi_count_df['creationDate'] == first_counts, 'isFirstCount'] = True
    
    # For each count, find the next count of the same location/material (if any)
    multi_count_df['nextCountDate'] = pd.NaT
    multi_count_df['nextCountVariance'] = np.nan
    multi_count_df['countCorrect'] = False  # Explicitly set to boolean
    
    # Process one location/material at a time to find next counts
    for (loc, mat, qual), group in multi_count_df.groupby(['locationName', 'materialName', 'qualification']):
        sorted_group = group.sort_values('creationDate')
        indices = sorted_group.index.tolist()
        
        for i in range(len(indices) - 1):
            current_idx = indices[i]
            next_idx = indices[i + 1]
            
            multi_count_df.loc[current_idx, 'nextCountDate'] = sorted_group.loc[next_idx, 'creationDate']
            multi_count_df.loc[current_idx, 'nextCountVariance'] = sorted_group.loc[next_idx, 'variance']
            multi_count_df.loc[current_idx, 'countCorrect'] = sorted_group.loc[next_idx, 'absoluteVariance'] <= threshold
    
    # Use multi_count_df directly 
    result_df = multi_count_df
    
    # Identify locations that passed (at least one count within threshold)
    location_passed = result_df.groupby(['locationName', 'materialName', 'qualification'])['withinThreshold'].any().reset_index()
    location_passed.columns = ['locationName', 'materialName', 'qualification', 'locationPassed']
    
    # Merge back to get locationPassed flag
    result_df = pd.merge(result_df, location_passed, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Filter to only include locations that passed (had at least one count within threshold)
    analysis_df = result_df[result_df['locationPassed']].copy()
    
    # By User - using ORIGINAL full dataframe for total counts
    user_metrics = []
    for user, group in analysis_df.groupby('userName'):
        # Total counts from the ORIGINAL full dataframe
        total_counts = df[df['userName'] == user].shape[0]
        
        # Convert to int to avoid boolean serialization
        incorrect_counts = int(sum((~group['withinThreshold']) & (~group['countCorrect'])))
        user_metrics.append({
            'userName': user,
            'totalCounts': total_counts,
            'incorrectCounts': incorrect_counts,
            'incorrectPercentage': round(incorrect_counts / total_counts * 100, 2) if total_counts > 0 else 0
        })
    
    # By Day - using ORIGINAL full dataframe for total counts
    analysis_df['countDate'] = analysis_df['creationDate'].dt.date
    day_metrics = []
    for day, group in analysis_df.groupby('countDate'):
        # Total counts from the ORIGINAL full dataframe for this date
        total_counts = df[pd.to_datetime(df['creationDate'], format='ISO8601').dt.date == day].shape[0]
        
        # Convert to int to avoid boolean serialization
        incorrect_counts = int(sum((~group['withinThreshold']) & (~group['countCorrect'])))
        day_metrics.append({
            'date': day.strftime('%Y-%m-%d'),
            'totalCounts': total_counts,
            'incorrectCounts': incorrect_counts,
            'incorrectPercentage': round(incorrect_counts / total_counts * 100, 2) if total_counts > 0 else 0
        })
    
    # Overall metrics - using ORIGINAL full dataframe for total counts
    total_analyzed_counts = df.shape[0]
    
    # Convert to int to avoid boolean serialization
    total_incorrect = int(sum((~analysis_df['withinThreshold']) & (~analysis_df['countCorrect'])))
    
    # Return the results
    return {
        'byUser': sorted(user_metrics, key=lambda x: x['incorrectCounts'], reverse=True),
        'byDay': sorted(day_metrics, key=lambda x: x['date']),
        'overall': {
            'totalCounts': total_analyzed_counts,
            'incorrectCounts': total_incorrect,
            'incorrectPercentage': round(total_incorrect / total_analyzed_counts * 100, 2) if total_analyzed_counts > 0 else 0
        }
    }

def get_incorrect_counts_details(df, threshold=2, only_incorrect=False):
    """
    Returns detailed information about incorrect counts
    
    Args:
        df: DataFrame containing count transactions
        threshold: The threshold for acceptable count variance (default: 2)
        only_incorrect: If True, only return incorrect counts
        
    Returns:
        List of count details
    """
    # Make a copy to avoid modifying the original
    counts_df = df.copy().infer_objects()
    
    # Convert creation date to datetime for proper sorting/comparison
    counts_df['creationDate'] = pd.to_datetime(counts_df['creationDate'], format='ISO8601')
    
    # Sort by location, material, and creation date
    counts_df = counts_df.sort_values(['locationName', 'materialName', 'qualification', 'creationDate'])
    
    # Add a column for count variance - absolute difference between requested and confirmed
    counts_df['variance'] = counts_df['quantityExpected'] - counts_df['quantityConfirmed']
    counts_df['absoluteVariance'] = np.abs(counts_df['variance'])
    
    # Identify if count is within threshold
    counts_df['withinThreshold'] = counts_df['absoluteVariance'] <= threshold
    
    # Group by location to count how many times each location was counted
    location_counts = counts_df.groupby(['locationName', 'materialName', 'qualification']).size().reset_index(name='timesLocationCounted')
    
    # Merge this information back to the main dataframe
    counts_df = pd.merge(counts_df, location_counts, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Only consider locations that were counted multiple times
    multi_count_df = counts_df[counts_df['timesLocationCounted'] > 1].copy()
    
    # Identify the first count for each location/material combination
    multi_count_df['isFirstCount'] = False
    first_counts = multi_count_df.groupby(['locationName', 'materialName', 'qualification'])['creationDate'].transform('min')
    multi_count_df.loc[multi_count_df['creationDate'] == first_counts, 'isFirstCount'] = True
    
    # For each count, find the next count of the same location/material (if any)
    multi_count_df['nextCountDate'] = pd.NaT
    multi_count_df['nextCountVariance'] = np.nan
    multi_count_df['countCorrect'] = False  # Explicitly set to boolean
    
    # Process one location/material at a time to find next counts
    for (loc, mat, qual), group in multi_count_df.groupby(['locationName', 'materialName', 'qualification']):
        sorted_group = group.sort_values('creationDate')
        indices = sorted_group.index.tolist()
        
        for i in range(len(indices) - 1):
            current_idx = indices[i]
            next_idx = indices[i + 1]
            
            multi_count_df.loc[current_idx, 'nextCountDate'] = sorted_group.loc[next_idx, 'creationDate']
            multi_count_df.loc[current_idx, 'nextCountVariance'] = sorted_group.loc[next_idx, 'variance']
            multi_count_df.loc[current_idx, 'countCorrect'] = sorted_group.loc[next_idx, 'absoluteVariance'] <= threshold
    
    # Identify locations that passed (at least one count within threshold)
    location_passed = multi_count_df.groupby(['locationName', 'materialName', 'qualification'])['withinThreshold'].any().reset_index()
    location_passed.columns = ['locationName', 'materialName', 'qualification', 'locationPassed']
    
    # Merge back to get locationPassed flag
    multi_count_df = pd.merge(multi_count_df, location_passed, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Filter to only include locations that passed
    analysis_df = multi_count_df[multi_count_df['locationPassed']].copy()
    
    # If only_incorrect is True, filter to only show incorrect counts
    if only_incorrect:
        analysis_df = analysis_df[(~analysis_df['withinThreshold']) & (~analysis_df['countCorrect'])]
    
    # Prepare the detailed output
    details = []
    for _, row in analysis_df.iterrows():
        # Convert boolean values to integers to ensure JSON serializability
        detail = {
            'date': row['creationDate'].isoformat(),
            'userName': row['userName'],
            'locationName': row['locationName'],
            'materialName': row['materialName'],
            'qualification': row['qualification'],
            'qtyRequested': row['quantityExpected'],
            'qtyConfirmed': row['quantityConfirmed'],
            'variance': row['variance'],
            'withinThreshold': int(row['withinThreshold']),  # Convert to integer
            'nextCountDate': row['nextCountDate'].isoformat() if pd.notna(row['nextCountDate']) else None,
            'nextCountVariance': float(row['nextCountVariance']) if pd.notna(row['nextCountVariance']) else None,
            'countCorrect': int(row['countCorrect']) if pd.notna(row['countCorrect']) else None,  # Convert to integer
            'timesLocationCounted': row['timesLocationCounted'],
            'isFirstCount': int(row['isFirstCount']),  # Convert to integer
            'isIncorrect': int(not row['withinThreshold'] and not row['countCorrect'])  # Convert to integer
        }
        details.append(detail)
    
    return details