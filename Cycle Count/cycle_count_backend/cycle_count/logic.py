import logging
from datetime import datetime, timedelta
from cycle_count.itempath_api import create_count_order_by_location
from cycle_count.sql_query import run_PPGLocationQuery
import pandas as pd
import numpy as np

def get_shelves_for_count_orders(df_inventory, num_orders, bool_numIsLocationsOrJerseys=True, bool_useShelvesOrLocations=True):
    """
    Calculate which shelves to include in count orders using a running total algorithm.
    
    Args:
        df_inventory: DataFrame containing either jersey or component inventory (already sorted)
        num_orders: Target number of locations or jerseys to count
        bool_numIsLocationsOrJerseys: If True, count is based on locations; if False, count is based on jersey quantity
        
    Returns:
        DataFrame containing the inventory items from selected shelves for counting
    """
    # Check if dataframe is empty
    if df_inventory.empty or num_orders <= 0:
        logging.info("Empty inventory dataframe or no orders requested")
        return pd.DataFrame()
    
    # Check if required columns exist
    if not bool_useShelvesOrLocations:
        df_inventory['UQ_Shelf'] = df_inventory['UQ_Location']

    required_columns = ["UQ_Shelf", "UQ_Location", "Default Count Rank", "QuantityCurrent"]
    for col in required_columns:
        if col not in df_inventory.columns:
            logging.error(f"Missing required column: {col}")
            return pd.DataFrame()
    
    try:
        # Get unique shelves with their location counts, maintaining sort order
        shelves_df = df_inventory.groupby("UQ_Shelf").agg({
            "UQ_Location": "nunique",
            "Default Count Rank": "first",  # Keep the rank for sorting
            "QuantityCurrent": "sum"
        }).reset_index()
        
        # Rename for clarity
        shelves_df = shelves_df.rename(columns={"UQ_Location": "LocationCount"})    
        
        # Sort by Default Count Rank (already sorted in df_inventory, but ensuring it's maintained)
        shelves_df = shelves_df.sort_values("Default Count Rank")

        # Initialize tracking variables
        running_total = 0
        shelves_to_count = []
        
        # Determine which column to use for counting based on the bool parameter
        count_column = "LocationCount" if bool_numIsLocationsOrJerseys else "QuantityCurrent"
        
        # Iterate through shelves
        for _, row in shelves_df.iterrows():
            shelf_id = row["UQ_Shelf"]
            shelf_count = row[count_column]
            
            # Add current shelf's count to running total
            running_total += shelf_count
            shelves_to_count.append(shelf_id)
            
            # If we've reached or exceeded our target, stop adding shelves
            if running_total >= num_orders:
                break
        
        # Return all inventory items from the selected shelves
        return df_inventory[df_inventory["UQ_Shelf"].isin(shelves_to_count)]
    except Exception as e:
        logging.error(f"Error in get_shelves_for_count_orders: {e}")
        return pd.DataFrame()

def split_large_orders(df, max_quantity=100):
    """
    Highly optimized function to split large count orders so no order exceeds max_quantity items.
    Based on UQ_Shelf column for determining if orders should be split.
    
    Args:
        df: DataFrame containing count orders
        max_quantity: Maximum number of items per order
        
    Returns:
        DataFrame with large orders split into multiple smaller orders
    """
    import time
    start_time = time.time()
    
    print(f"Starting split_large_orders with {len(df)} rows")
    
    if df.empty:
        print("Empty DataFrame received, returning as is")
        return df
    
    # Group by UQ_Shelf and calculate total quantities more efficiently
    print("Grouping data by UQ_Shelf...")
    shelf_stats = df.groupby("UQ_Shelf").agg({
        "QuantityCurrent": "sum"
    }).reset_index()
    
    print(f"Found {len(shelf_stats)} unique shelves")
    
    # Identify shelves that need splitting (total quantity > max_quantity)
    shelves_to_split = shelf_stats[shelf_stats["QuantityCurrent"] > max_quantity]["UQ_Shelf"].tolist()
    shelves_ok = shelf_stats[shelf_stats["QuantityCurrent"] <= max_quantity]["UQ_Shelf"].tolist()
    
    print(f"Shelves requiring splitting: {len(shelves_to_split)} of {len(shelf_stats)}")
    
    # Process shelves that don't need splitting all at once
    print("Processing shelves that don't need splitting...")
    no_split_mask = df["UQ_Shelf"].isin(shelves_ok)
    no_split_df = df[no_split_mask]
    
    # Initialize list for split results
    split_chunks = []
    
    # Only process shelves that need splitting
    if shelves_to_split:
        print(f"Processing {len(shelves_to_split)} shelves that need splitting...")
        
        for i, shelf_id in enumerate(shelves_to_split):
            # Get items for this shelf - do this filtering once
            shelf_items = df[df["UQ_Shelf"] == shelf_id]
            total_quantity = shelf_items["QuantityCurrent"].sum()
            item_count = len(shelf_items)
            
            print(f"Processing shelf to split {i+1}/{len(shelves_to_split)}: {shelf_id} with {total_quantity} total items across {item_count} locations")
            
            # Skip if somehow this shelf doesn't actually need splitting
            if total_quantity <= max_quantity:
                print(f"  - Actually doesn't need splitting: {shelf_id}")
                split_chunks.append(shelf_items)
                continue
            
            # Calculate number of splits needed
            num_splits = int(np.ceil(total_quantity / max_quantity))
            
            # Sort items once before splitting - more predictable results
            shelf_items = shelf_items.sort_values("UQ_Location")
            quantities = shelf_items["QuantityCurrent"].values
            cumulative = np.cumsum(quantities)
            
            # Skip processing if we have no quantities (shouldn't happen but better safe)
            if len(cumulative) == 0 or cumulative[-1] == 0:
                print(f"  - Warning: Shelf {shelf_id} has no items or zero quantity")
                continue
                
            # Pre-calculate all target quantities for each split point
            # This avoids empty chunks by ensuring we're splitting based on the actual data
            targets = [((i * cumulative[-1]) / num_splits) for i in range(1, num_splits)]
            
            # Find indices where we should split
            split_indices = np.searchsorted(cumulative, targets)
            
            # Check if we have valid split points
            if len(split_indices) == 0:
                print(f"  - Warning: No valid split points for {shelf_id}, keeping as single chunk")
                # Create a copy if we need to modify it
                chunk = shelf_items.copy()
                chunk["UQ_Shelf"] = shelf_id + "_1"
                split_chunks.append(chunk)
                continue
                
            # Make sure we have no duplicate split points
            split_indices = np.unique(split_indices)
            
            # Check if any split points would create empty chunks
            if np.any(np.diff(np.concatenate([[0], split_indices, [len(shelf_items)]])) == 0):
                print(f"  - Warning: Split would create empty chunks for {shelf_id}, adjusting")
                # Recompute with fewer splits to avoid empty chunks
                num_splits = min(num_splits, len(shelf_items))
                targets = [((i * cumulative[-1]) / num_splits) for i in range(1, num_splits)]
                split_indices = np.searchsorted(cumulative, targets)
                split_indices = np.unique(split_indices)
                
            print(f"  - Splitting {shelf_id} into {num_splits} chunks at indices {split_indices}")
            
            # Process chunks
            splits = np.concatenate([[0], split_indices, [len(shelf_items)]])
            for i in range(len(splits) - 1):
                start_idx = splits[i]
                end_idx = splits[i+1]
                
                # Skip empty chunks
                if start_idx == end_idx:
                    print(f"  - Skipping empty chunk {i+1}")
                    continue
                    
                # Get chunk and make a copy
                chunk = shelf_items.iloc[start_idx:end_idx].copy()
                
                # Generate new shelf ID with suffix
                suffix = f"_{i+1}"
                chunk["UQ_Shelf"] = shelf_id + suffix
                
                chunk_sum = chunk["QuantityCurrent"].sum()
                print(f"  - Chunk {i+1}: rows={len(chunk)}, quantity={chunk_sum}")
                
                split_chunks.append(chunk)
    
    # Combine both sets of results
    if split_chunks:
        print(f"Combining {len(split_chunks)} split chunks with {len(no_split_df)} unsplit rows...")
        result_df = pd.concat([no_split_df] + split_chunks, ignore_index=True)
    else:
        print("No splitting was needed, returning original data")
        result_df = no_split_df
    
    end_time = time.time()
    print(f"Finished processing. Total time: {end_time - start_time:.4f} seconds")
    print(f"Original rows: {len(df)}, Result rows: {len(result_df)}")
    
    return result_df

def run_cycle_count(api_base_url, access_token, NUM_JERSEYCOUNTORDERS, NUM_COMPONENTCOUNTORDERS, optStorageUnits=[], optQualifers=[], opt_locations=[], additionalPrefix=""):
    """
    Main function that executes the cycle counting logic with VIP + FIFO:
    """
    # I want the batches to be of this format YYMMDD.Jersey/Component.CC.Rank
    dateToUse = datetime.now()

    # Adjust dateToUse + 1 day
    dateToUse = dateToUse + timedelta(days=0)

    BATCH_PREFIX = dateToUse.strftime("%y%m%d") + ".%WHS%%TYPE%" + "Count.%RANK%"
    MAX_ITEMS_PER_ORDER = 1000  # Maximum number of items per count order
    MAX_JERSEYS_PER_ORDER = 250
    MAX_COMPONENTS_PER_ORDER = 2000

    logging.info("Starting cycle count run.")

    # 1) Fetch all location contents from the API, with VIP labeling
    df_inventory = run_PPGLocationQuery()

    # Filter out all StorageunitName that start with BorderWorx
    df_inventory = df_inventory[~df_inventory["StorageunitName"].str.startswith("BorderWorx")]

    df_inventory["UQ_Shelf"] = df_inventory["StorageunitName"] + "_" + df_inventory["CarrierName"] + "_" + df_inventory["ShelfName"]  # Create uq shelf string for orders
    df_inventory["UQ_Shelf"] = df_inventory["UQ_Shelf"].str.replace(" ", "")  # Replace all spaces with blank
    
    df_inventory["UQ_Location"] = df_inventory["UQ_Shelf"] + "_" + df_inventory["LocationName"]  # Create uq location string for orders
    df_inventory["UQ_Location"] = df_inventory["UQ_Location"].str.replace(" ", "")  # Replace all spaces with blank

    df_inventory = df_inventory[["UQ_Shelf"] + ["UQ_Location"] + [col for col in df_inventory.columns if col not in ["UQ_Shelf", "UQ_Location"]]]

    # Create effective count date. if the Countdate is blank or null use the oldest of the Storagedate and Putdate
    df_inventory["Effective Count Date"] = df_inventory["Countdate"].fillna(df_inventory[["Storagedate", "Putdate"]].min(axis=1))

    # If the ItemTypeCode is in '121' or '127' then set Inventory Type Classification to Jerseys/Garments else to Components
    df_inventory["isJerseyOrGarment"] = df_inventory["ItemTypeCode"].apply(lambda x: x in ['121', '127']) & ~(df_inventory["StorageunitName"].str.startswith("V") | (df_inventory["StorageunitName"] == "G5") | (df_inventory["StorageunitName"] == "PPG Receiving")) & ~df_inventory["Info1"].str.contains("Patch", case=False, na=False)

    # Conditionally update UQ_Shelf only for rows where isJerseyOrGarment is False
    df_inventory['UQ_Shelf'] = df_inventory.apply(
        lambda row: row['UQ_Location'] if row['isJerseyOrGarment'] == False else row['UQ_Shelf'], 
        axis=1
    )
    
    #comment out temporarily spli_large_orders
    #df_inventory = split_large_orders(df_inventory, MAX_ITEMS_PER_ORDER)

    # From df_inventory create a new dataframe df_shelfPriority aggregate the minimum effective count date
    df_shelfPriority = df_inventory.groupby(["StorageunitName", "UQ_Shelf", "Qualification", "isJerseyOrGarment"]).agg({"Effective Count Date": "min", "LocationId": "count"}).reset_index()
    df_shelfPriority = df_shelfPriority.rename(columns={"LocationId": "# of Locations"})

    # Sort df_shelfPriority by effective count date and assign a default count rank to it (1 being most important)
    df_shelfPriority = df_shelfPriority.sort_values(by=["Effective Count Date"], ascending=True)
    df_shelfPriority["Default Count Rank"] = range(1, 1 + len(df_shelfPriority))

    # Take default count rank from df_shelfPriority and assign it to df_inventory using the UQ_Shelf column
    df_inventory = df_inventory.merge(df_shelfPriority[["UQ_Shelf", "Default Count Rank"]], on="UQ_Shelf", how="left")

    # Sort df_inventory by default count rank and UQ_Location
    df_inventory = df_inventory.sort_values(by=["Default Count Rank", "UQ_Location"], ascending=True)

    # Drop duplicates in UQ_Location column, keeping the first occurrence
    df_inventory = df_inventory.drop_duplicates(subset=["UQ_Location"], keep="first")

    # Split df_inventory into jersey and component inventory
    df_jerseyInventory = df_inventory[df_inventory["isJerseyOrGarment"]]
    df_componentInventory = df_inventory[~df_inventory["isJerseyOrGarment"]]

    if len(optStorageUnits) > 0:
        df_jerseyInventory = df_jerseyInventory[df_jerseyInventory["StorageunitName"].isin(optStorageUnits)]
        df_componentInventory = df_componentInventory[df_componentInventory["StorageunitName"].isin(optStorageUnits)] 

    if len(optQualifers) > 0:
        df_jerseyInventory = df_jerseyInventory[df_jerseyInventory["Qualification"].isin(optQualifers)]
        df_componentInventory = df_componentInventory[df_componentInventory["Qualification"].isin(optQualifers)]

    if len(opt_locations) > 0:
        df_jerseyInventory = df_jerseyInventory[df_jerseyInventory["LocationName"].isin(opt_locations)]
        df_componentInventory = df_componentInventory[df_componentInventory["LocationName"].isin(opt_locations)]

    # Output inventory to temp.csv df
    df_inventory.to_csv("temp_inventoryReport.csv", index=False)
    df_shelfPriority.to_csv("temp_shelfPriority.csv", index=False)

    df_jerseyInventory.to_csv("temp_jerseyInventory.csv", index=False)
    df_componentInventory.to_csv("temp_componentInventory.csv", index=False)

    print("pre shelf count order")

    # Now df_jerseyInventory and df_componentInventory are sorted by default count rank and UQ_Location
    # Get shelves for count orders
    countOrders_jerseys = get_shelves_for_count_orders(df_jerseyInventory, NUM_JERSEYCOUNTORDERS, False)
    countOrders_components = get_shelves_for_count_orders(df_componentInventory, NUM_COMPONENTCOUNTORDERS, False, False)

    print("post shelf count order")

    # Check if resulting dataframes are empty
    if countOrders_jerseys.empty:
        logging.warning("No jersey items selected for counting")
        countOrders_jerseys = pd.DataFrame(columns=df_jerseyInventory.columns)

    if countOrders_components.empty:
        logging.warning("No component items selected for counting")
        countOrders_components = pd.DataFrame(columns=df_componentInventory.columns)

    # Create explicit copies to avoid SettingWithCopyWarning
    countOrders_jerseys = countOrders_jerseys.copy() if not countOrders_jerseys.empty else countOrders_jerseys
    countOrders_components = countOrders_components.copy() if not countOrders_components.empty else countOrders_components

    print("pre naming")
    print("Jersey columns:", df_jerseyInventory.columns.tolist())
    print("Component columns:", df_componentInventory.columns.tolist())
    print("Jersey count orders shape:", countOrders_jerseys.shape)
    print("Component count orders shape:", countOrders_components.shape)

    # Process jersey batch names only if dataframe is not empty
    if not countOrders_jerseys.empty and "Default Count Rank" in countOrders_jerseys.columns:
        countOrders_jerseys["BatchName"] = countOrders_jerseys["Default Count Rank"].apply(
            lambda x: BATCH_PREFIX.replace("%TYPE%", additionalPrefix + "Jersey").replace("%RANK%", str(int(x))) if pd.notna(x) else BATCH_PREFIX.replace("%TYPE%", additionalPrefix + "Jersey").replace("%RANK%", "Unknown")
        )
        countOrders_jerseys["BatchName"] = countOrders_jerseys.apply(
            lambda row: row["BatchName"].replace("%WHS%", str(row["StorageunitName"])), 
            axis=1
        )

    print("@Components")

    # Process component batch names only if dataframe is not empty
    if not countOrders_components.empty and "Default Count Rank" in countOrders_components.columns:
        countOrders_components["BatchName"] = countOrders_components["Default Count Rank"].apply(
            lambda x: BATCH_PREFIX.replace("%TYPE%", additionalPrefix + "Component").replace("%RANK%", str(int(x))) if pd.notna(x) else BATCH_PREFIX.replace("%TYPE%", additionalPrefix + "Component").replace("%RANK%", "Unknown")
        )
        countOrders_components["BatchName"] = countOrders_components.apply(
            lambda row: row["BatchName"].replace("%WHS%", str(row["StorageunitName"])), 
            axis=1
        )


    
    print("post naming")
    print(countOrders_jerseys)
    print(countOrders_components)

    # Initialize a dictionary to track running counts for each BatchName
    batch_counters = {}

    # Define a function to generate the order name with running count
    def generate_order_name(batch_name):
        if batch_name not in batch_counters:
            batch_counters[batch_name] = 1
        else:
            batch_counters[batch_name] += 1
        
        return batch_counters[batch_name]

    # Apply the function to create the OrderName column only if dataframes are not empty
    if not countOrders_jerseys.empty and "BatchName" in countOrders_jerseys.columns:
        countOrders_jerseys['OrderRowNum'] = countOrders_jerseys['BatchName'].apply(generate_order_name)
        countOrders_jerseys['OrderName'] = countOrders_jerseys['BatchName'] + "." + countOrders_jerseys['OrderRowNum'].astype(str)

    if not countOrders_components.empty and "BatchName" in countOrders_components.columns:
        countOrders_components['OrderRowNum'] = countOrders_components['BatchName'].apply(generate_order_name)
        countOrders_components['OrderName'] = countOrders_components['BatchName'] + "." + countOrders_components['OrderRowNum'].astype(str)

    # Output to CSV
    countOrders_jerseys.to_csv("temp_jerseyCountOrders.csv", index=False)
    countOrders_components.to_csv("temp_componentCountOrders.csv", index=False)

    logging.info(f"Fetched a total of {len(df_inventory)} items. Now sorting by Priorities & FIFO")

    # 3) Create count orders for specification in function
    orderDf = pd.DataFrame(columns=["BatchName", "LocationId"])  # Initialize empty DataFrame

    # Process jerseys only if not empty
    if not countOrders_jerseys.empty and "BatchName" in countOrders_jerseys.columns:
        # Convert LocationId to a string
        countOrders_jerseys["LocationId"] = countOrders_jerseys["LocationId"].astype(str)
        # Create a final DF that takes the batch name and concatenates the location IDs as a string into an array
        jersey_orderDf = countOrders_jerseys.groupby("BatchName").agg({"LocationId": lambda x: list(x)}).reset_index()
        orderDf = pd.concat([orderDf, jersey_orderDf], axis=0)
    
    # Process components only if not empty
    if not countOrders_components.empty and "BatchName" in countOrders_components.columns:
        # Convert LocationId to a string
        countOrders_components["LocationId"] = countOrders_components["LocationId"].astype(str)
        # Create a final DF that takes the batch name and concatenates the location IDs as a string into an array
        component_orderDf = countOrders_components.groupby("BatchName").agg({"LocationId": lambda x: list(x)}).reset_index()
        orderDf = pd.concat([orderDf, component_orderDf], axis=0)

    # Reset the index
    orderDf = orderDf.reset_index(drop=True)
    failArr = []
    i = 0
    while i < len(orderDf):
        batchName = orderDf.iloc[i]["BatchName"]
        location_ids_to_count = orderDf.iloc[i]["LocationId"]

        result = create_count_order_by_location(
            api_base_url=api_base_url,
            access_token=access_token,
            order_name=batchName,
            location_ids=location_ids_to_count,
            priority=3,
            debug=True
        )

        # Check if the order already exists or has no order lines
        if result.get("already_exists", False):
            print(f"Order '{result['order_name']}' already exists. Continuing to next order.")
        elif result.get("no_order_lines", False):
            print(f"Order '{result['order_name']}' has no order lines. Continuing to next order.")
        elif result.get("failed", False):
            print(f"Order '{result['order_name']}' failed.")
            failArr.append(result['order_name'])
        else:
            print(result)
        
        i += 1

    # Create the final output array
    createdOrderArr = pd.DataFrame(columns=["OrderName", "UQ_Shelf", "Effective Count Date"])

    
    
    if not countOrders_jerseys.empty and "BatchName" in countOrders_jerseys.columns:
        jersey_created_orders = countOrders_jerseys.groupby("BatchName").agg({"UQ_Shelf": "first", "Effective Count Date": "min"}).reset_index()
        jersey_created_orders["BarCode"] = "*" + jersey_created_orders["BatchName"] + "*"
        jersey_created_orders = jersey_created_orders.rename(columns={"BatchName": "OrderName"})
        createdOrderArr = pd.concat([createdOrderArr, jersey_created_orders], axis=0)
    
    if not countOrders_components.empty and "BatchName" in countOrders_components.columns:
        component_created_orders = countOrders_components.groupby("BatchName").agg({"UQ_Shelf": "first", "Effective Count Date": "min"}).reset_index()
        component_created_orders["BarCode"] = "*" + component_created_orders["BatchName"] + "*"
        component_created_orders = component_created_orders.rename(columns={"BatchName": "OrderName"})
        createdOrderArr = pd.concat([createdOrderArr, component_created_orders], axis=0)

    print(createdOrderArr)
    # Remove any failed orders from createdOrderArr
    if failArr != [] and not createdOrderArr.empty:
        print("clearing")
        print(failArr)
        createdOrderArr = createdOrderArr[~createdOrderArr["OrderName"].isin(failArr)]

    # Output to CSV
    createdOrderArr.to_csv("temp_createdOrders.csv", index=False)
    logging.info(f"Cycle count run complete. Orders created: {len(createdOrderArr)}")

    return createdOrderArr