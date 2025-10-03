# cycle_count/coverage_analysis.py
import pandas as pd
from datetime import datetime
import logging
from cycle_count.sql_query import run_PPGLocationQuery

def analyze_inventory_coverage(target_date=None):
    """
    Analyzes inventory coverage based on count dates for each storage unit/qualifier/item type combination.
    
    Args:
        target_date (str, optional): The reference date in 'YYYY-MM-DD' format. 
                                    If None, defaults to 60 days in the past from current date.
    
    Returns:
        DataFrame containing storage unit/qualifier/item type combinations with coverage percentages
        and a dict with summary statistics
    """
    logging.info(f"Starting inventory coverage analysis from date: {target_date}")
    
    # Convert target_date to datetime if provided
    if target_date:
        try:
            reference_date = pd.to_datetime(target_date)
        except ValueError:
            logging.error(f"Invalid date format: {target_date}. Using date from 60 days ago instead.")
            reference_date = pd.to_datetime((datetime.now() - pd.Timedelta(days=60)).date())
    else:
        # Default to 60 days in the past from current date
        reference_date = pd.to_datetime((datetime.now() - pd.Timedelta(days=60)).date())
    
    logging.info(f"Using reference date: {reference_date}")
    
    # Fetch all location data
    df_inventory = run_PPGLocationQuery()
    
    # Clean up data
    # Filter out BorderWorx items
    df_inventory = df_inventory[~df_inventory["StorageunitName"].str.startswith("BorderWorx")]
    
    # Convert date columns to datetime for comparison - handle NaT values
    df_inventory["Countdate"] = pd.to_datetime(df_inventory["Countdate"], errors='coerce')
    df_inventory["Storagedate"] = pd.to_datetime(df_inventory["Storagedate"], errors='coerce')
    df_inventory["Putdate"] = pd.to_datetime(df_inventory["Putdate"], errors='coerce')
    
    # Create effective put/storage date (the later of Storagedate and Putdate)
    # Handle NaT values by using only non-NaT values or setting to NaT if both are NaT
    df_inventory["Effective Put Date"] = df_inventory.apply(
        lambda row: max(
            [d for d in [row["Storagedate"], row["Putdate"]] if pd.notna(d)], 
            default=pd.NaT
        ), 
        axis=1
    )
    
    # Ensure ItemType has a value for grouping
    df_inventory["ItemType"] = df_inventory["ItemType"].fillna("Unknown")
    
    # Add flags for locations counted or put after the reference date
    # Handle NaT values properly by using False for NaT comparisons
    df_inventory["CountedAfterReferenceDate"] = df_inventory.apply(
        lambda row: pd.notna(row["Countdate"]) and row["Countdate"] >= reference_date, 
        axis=1
    )
    
    df_inventory["PutAfterReferenceDate"] = df_inventory.apply(
        lambda row: pd.notna(row["Effective Put Date"]) and row["Effective Put Date"] >= reference_date,
        axis=1
    )
    
    # Calculate verified items - either counted after reference date OR put after reference date
    df_inventory["VerifiedAfterReferenceDate"] = df_inventory["CountedAfterReferenceDate"] | df_inventory["PutAfterReferenceDate"]
    
    # Drop locations without items (empty locations)
    df_inventory = df_inventory[~df_inventory["ItemCode"].isnull() & ~df_inventory["ItemCode"].eq("")]
    
    # Ensure Quantity column exists (assuming Quantity is the column name for item quantities)
    # If it doesn't exist, create it with a default value of 1 for each row
    if "Quantity" not in df_inventory.columns:
        logging.warning("Quantity column not found in data. Using default value of 1 per item.")
        df_inventory["Quantity"] = 1
    
    # Group by storage unit, qualification, and item type
    results = []
    
    # Get unique combinations of storage unit, qualification, and item type
    # Using ItemType string, not a code, for grouping
    unique_combos = df_inventory[["StorageunitName", "Qualification", "ItemType"]].drop_duplicates()
    
    # For each combination, calculate coverage statistics
    for _, row in unique_combos.iterrows():
        storage_unit = row["StorageunitName"]
        qualifier = row["Qualification"]
        item_type = row["ItemType"]
        
        # Filter for this storage unit, qualifier, and item type
        combo_data = df_inventory[
            (df_inventory["StorageunitName"] == storage_unit) & 
            (df_inventory["Qualification"] == qualifier) &
            (df_inventory["ItemType"] == item_type)
        ]
        
        # Calculate locations
        total_locations = len(combo_data["LocationId"].unique())
        
        # Get unique location IDs for each category
        counted_location_ids = set(combo_data[combo_data["CountedAfterReferenceDate"]]["LocationId"].unique())
        put_location_ids = set(combo_data[combo_data["PutAfterReferenceDate"]]["LocationId"].unique())
        verified_location_ids = set(combo_data[combo_data["VerifiedAfterReferenceDate"]]["LocationId"].unique())
        
        # Count locations for each category (without double-counting)
        counted_locations = len(counted_location_ids)
        put_locations = len(put_location_ids - counted_location_ids)  # Only count locations put but not counted
        verified_locations = len(verified_location_ids)
        
        # FIXED: Calculate items by summing quantities instead of counting rows
        total_items = combo_data["QuantityCurrent"].sum()
        
        # Sum items for each category (without double-counting)
        counted_items = combo_data[combo_data["CountedAfterReferenceDate"]]["QuantityCurrent"].sum()
        put_items = combo_data[~combo_data["CountedAfterReferenceDate"] & combo_data["PutAfterReferenceDate"]]["QuantityCurrent"].sum()
        verified_items = combo_data[combo_data["VerifiedAfterReferenceDate"]]["QuantityCurrent"].sum()
        
        # Calculate percentages
        location_coverage = round((verified_locations / total_locations * 100), 2) if total_locations > 0 else 0
        item_coverage = round((verified_items / total_items * 100), 2) if total_items > 0 else 0
        
        # Store results
        results.append({
            "StorageUnit": storage_unit,
            "Qualification": qualifier,
            "ItemType": item_type,
            "TotalLocations": total_locations,
            "CountedLocations": counted_locations,
            "PutLocations": put_locations,
            "VerifiedLocations": verified_locations,
            "LocationCoverage": location_coverage,
            "TotalItems": total_items,
            "CountedItems": counted_items,
            "PutItems": put_items,
            "VerifiedItems": verified_items,
            "ItemCoverage": item_coverage,
            "ReferenceDate": reference_date.strftime('%Y-%m-%d')
        })
    
    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    
    # Sort by location coverage (descending) so highest coverage items appear first
    results_df = results_df.sort_values("LocationCoverage", ascending=False)
    
    # Calculate overall statistics
    total_all_locations = results_df["TotalLocations"].sum()
    counted_all_locations = results_df["CountedLocations"].sum()
    put_all_locations = results_df["PutLocations"].sum()
    verified_all_locations = results_df["VerifiedLocations"].sum()
    overall_location_coverage = round((verified_all_locations / total_all_locations * 100), 2) if total_all_locations > 0 else 0
    
    total_all_items = results_df["TotalItems"].sum()
    counted_all_items = results_df["CountedItems"].sum()
    put_all_items = results_df["PutItems"].sum()
    verified_all_items = results_df["VerifiedItems"].sum()
    overall_item_coverage = round((verified_all_items / total_all_items * 100), 2) if total_all_items > 0 else 0
    
    # Create summary dict
    summary = {
        "overallLocationCoverage": overall_location_coverage,
        "overallItemCoverage": overall_item_coverage,
        "totalLocations": int(total_all_locations),
        "countedLocations": int(counted_all_locations),
        "putLocations": int(put_all_locations),
        "verifiedLocations": int(verified_all_locations),
        "totalItems": int(total_all_items),
        "countedItems": int(counted_all_items),
        "putItems": int(put_all_items),
        "verifiedItems": int(verified_all_items),
        "referenceDate": reference_date.strftime('%Y-%m-%d'),
        "analysisDate": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logging.info(f"Coverage analysis complete. Overall location coverage: {overall_location_coverage}%, Overall item coverage: {overall_item_coverage}%")
    
    return results_df, summary

def get_coverage_since_date(date_str=None):
    """
    Endpoint-friendly function to get coverage analysis since a particular date.
    
    Args:
        date_str (str, optional): The reference date in 'YYYY-MM-DD' format.
    
    Returns:
        Dict containing coverage results and summary statistics
    """
    try:
        results_df, summary = analyze_inventory_coverage(date_str)
        
        # Convert DataFrame to dict for JSON serialization
        coverage_data = results_df.to_dict(orient='records')
        
        return {
            "success": True,
            "coverage": coverage_data,
            "summary": summary
        }
    except Exception as e:
        logging.error(f"Error in coverage analysis: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }