# cycle_count/logic.py
import logging
from datetime import datetime
from cycle_count.itempath_api import create_count_order_by_location
from cycle_count.sql_query import run_PPGLocationQuery
import pandas as pd

def get_shelves_for_count_orders(df_inventory, num_orders):
    """
    Calculate which shelves to include in count orders using a running total algorithm.
    
    Args:
        df_inventory: DataFrame containing either jersey or component inventory (already sorted)
        num_orders: Target number of locations to count
        
    Returns:
        DataFrame containing the inventory items from selected shelves for counting
    """
    if num_orders <= 0:
        return pd.DataFrame()  # Return empty dataframe if no orders requested
    
    # Get unique shelves with their location counts, maintaining sort order
    shelves_df = df_inventory.groupby("UQ_Shelf").agg({
        "UQ_Location": "nunique",
        "Default Count Rank": "first"  # Keep the rank for sorting
    }).reset_index()
    
    # Rename for clarity
    shelves_df = shelves_df.rename(columns={"UQ_Location": "LocationCount"})
    
    # Sort by Default Count Rank (already sorted in df_inventory, but ensuring it's maintained)
    shelves_df = shelves_df.sort_values("Default Count Rank")
    
    # Initialize tracking variables
    running_total = 0
    shelves_to_count = []
    
    # Iterate through shelves
    for _, row in shelves_df.iterrows():
        shelf_id = row["UQ_Shelf"]
        shelf_location_count = row["LocationCount"]
        
        # Add current shelf's locations to running total
        running_total += shelf_location_count
        shelves_to_count.append(shelf_id)
        
        # If we've reached or exceeded our target, stop adding shelves
        if running_total >= num_orders:
            break
    
    # Return all inventory items from the selected shelves
    return df_inventory[df_inventory["UQ_Shelf"].isin(shelves_to_count)]

def run_cycle_count(api_base_url, access_token, NUM_JERSEYCOUNTORDERS):
    """
    Main function that executes the cycle counting logic with VIP + FIFO:


    """
    NUM_COMPONENTORDERS=0

    #I want the batches to be of this format YYMMDD.Jersey/Component.CC.Rank
    BATCH_PREFIX = datetime.now().strftime("%y%m%d") + ".%TYPE%"+ "Count.%RANK%"

    logging.info("Starting cycle count run.")

    # 1) Fetch all location contents from the API, with VIP labeling
    df_inventory = run_PPGLocationQuery()

    #filter out all StorageunitName that strt with BorderWorx
    df_inventory = df_inventory[~df_inventory["StorageunitName"].str.startswith("BorderWorx")]

    df_inventory["UQ_Shelf"] = df_inventory["StorageunitName"] + "_" + df_inventory["CarrierName"] + "_" + df_inventory["ShelfName"] #create uq shelf string for orders
    df_inventory["UQ_Shelf"] = df_inventory["UQ_Shelf"].str.replace(" ", "") #replace all spaces with blank
    
    df_inventory["UQ_Location"] = df_inventory["UQ_Shelf"] + "_" + df_inventory["LocationName"]  #create uq location string for orders
    df_inventory["UQ_Location"] = df_inventory["UQ_Location"].str.replace(" ", "") #replace all spaces with blank

    df_inventory = df_inventory[["UQ_Shelf"] + ["UQ_Location"] + [col for col in df_inventory.columns if col not in ["UQ_Shelf","UQ_Location"]]]

    #create effective count date. if the Countdate is blank or null use the oldest of the Storagedate and Putdate
    df_inventory["Effective Count Date"] = df_inventory["Countdate"].fillna(df_inventory[["Storagedate", "Putdate"]].min(axis=1))

    #if the ItemTypeCode is in '121' or '127' then set Inventory Type Classification to Jerseys/Garments else to Components
    df_inventory["isJerseyOrGarment"] = df_inventory["ItemTypeCode"].apply(lambda x: x in ['121', '127']) & ~(df_inventory["StorageunitName"].str.startswith("V") | (df_inventory["StorageunitName"] == "G5") | (df_inventory["StorageunitName"] == "PPG Receiving")) & ~df_inventory["Info1"].str.contains("Patch", case=False, na=False)

    #from df_inventory create a new dataframe df_shelfPriority aggregate the minimum effective count date
    df_shelfPriority = df_inventory.groupby(["StorageunitName","UQ_Shelf","Qualification","isJerseyOrGarment"]).agg({"Effective Count Date": "min", "LocationId": "count"}).reset_index()
    df_shelfPriority = df_shelfPriority.rename(columns={"LocationId": "# of Locations"})

    #sort df_shelfPriority by effective count date and assign a default count rank to it (1 being most important)
    df_shelfPriority = df_shelfPriority.sort_values(by=["Effective Count Date"], ascending=True)
    df_shelfPriority["Default Count Rank"] = range(1, 1+len(df_shelfPriority))

    #take default count rank from df_shelfPriority and assign it to df_inventory using the UQ_Shelf column
    df_inventory = df_inventory.merge(df_shelfPriority[["UQ_Shelf", "Default Count Rank"]], on="UQ_Shelf", how="left")

    #sprt df_inventory by default count rank and UQ_Location
    df_inventory = df_inventory.sort_values(by=["Default Count Rank", "UQ_Location"], ascending=True)

    #split df_inentory into 
    df_jerseyInventory = df_inventory[df_inventory["isJerseyOrGarment"]]
    df_componentInventory = df_inventory[~df_inventory["isJerseyOrGarment"]]

    #ouput inventory to temp.csv df
    df_inventory.to_csv("temp_inventoryReport.csv", index=False)
    df_shelfPriority.to_csv("temp_shelfPriority.csv", index=False)

    df_jerseyInventory.to_csv("temp_jerseyInventory.csv", index=False)
    df_componentInventory.to_csv("temp_componentInventory.csv", index=False)

    #now df_jerseyInventory and df_componentInventory are sorted by default count rank and UQ_Location I want to figure out based on the number of jersey orders and component orders provided. We must count a shelf in full so we can go over or under depending on the number of locations in the shelf. we should do a running total algo
    #for each shelf in the inventory we should count the number of locations in the shelf and add that to the running total. If the running total is less than the number of orders we need to create we should add the shelf to the order list. If the running total is greater than the number of orders we need to create we should not add the shelf to the order list. If the running total is equal to the number of orders we need to create we should add the shelf to the order list and reset the running total to 0. We should do this for both jersey and component inventory

    countOrders_jerseys = get_shelves_for_count_orders(df_jerseyInventory, NUM_JERSEYCOUNTORDERS)

    # Create an explicit copy to avoid SettingWithCopyWarning
    countOrders_jerseys = countOrders_jerseys.copy()

    # Then assign the BatchName
    countOrders_jerseys["BatchName"] = countOrders_jerseys["Default Count Rank"].apply(
        lambda x: BATCH_PREFIX.replace("%TYPE%", "Jersey").replace("%RANK%", str(int(x))) if pd.notna(x) else BATCH_PREFIX.replace("%TYPE%", "Jersey").replace("%RANK%", "Unknown")
    )

    # Initialize a dictionary to track running counts for each BatchName
    batch_counters = {}

    # Define a function to generate the order name with running count
    def generate_order_name(batch_name):
        if batch_name not in batch_counters:
            batch_counters[batch_name] = 1
        else:
            batch_counters[batch_name] += 1
        
        return batch_counters[batch_name]

    # Apply the function to create the OrderName column
    countOrders_jerseys['OrderRowNum'] = countOrders_jerseys['BatchName'].apply(generate_order_name)
    countOrders_jerseys['OrderName'] = countOrders_jerseys['BatchName'] + "." + countOrders_jerseys['OrderRowNum'].astype(str)

    #to csv
    countOrders_jerseys.to_csv("temp_jerseyCountOrders.csv", index=False)

    logging.info(f"Fetched a total of {len(df_inventory)} items. Now sorting by Priorities & FIFO")

    # 3) Create count orders for specification in function'

    #convert LoctaionId to a string
    countOrders_jerseys["LocationId"] = countOrders_jerseys["LocationId"].astype(str)

    #create a final DF that takes the batch name and concatenates the location IDs as a string into an array
    orderDf = countOrders_jerseys.groupby("BatchName").agg({"LocationId": lambda x: list(x)}).reset_index()



    i = 0
    while i < len(orderDf):
        batchName = orderDf.iloc[i]["BatchName"]
        location_ids_to_count = orderDf.iloc[i]["LocationId"]

        x = create_count_order_by_location(
            api_base_url=api_base_url,
            access_token=access_token,
            order_name=batchName,
            location_ids=location_ids_to_count,
            priority=3,
            debug=True
        )

        print(x)
        
        i+= 1

    #do a unique on createdOrderArr
    createdOrderArr = countOrders_jerseys.groupby("BatchName").agg({"UQ_Shelf": "first", "Effective Count Date": "min"}).reset_index()

    #duplicate orderName column and prefix and suffix with a &
    createdOrderArr["BarCode"] = "*" + createdOrderArr["BatchName"] + "*"

    #rename BatchName to OrderName
    createdOrderArr = createdOrderArr.rename(columns={"BatchName": "OrderName"})

    #output to csv
    createdOrderArr.to_csv("temp_createdOrders.csv", index=False)

    logging.info(f"Cycle count run complete. Orders created: {len(createdOrderArr)}")

    return createdOrderArr
