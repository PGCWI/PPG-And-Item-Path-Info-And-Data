import pymssql
import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, Optional, List
from datetime import datetime, timedelta

#create a function to connect to the database
def connect_to_ppgDB():
    cnx = pymssql.connect(
        server='sql-srvr-01.silvercrystal.com',
        user=os.environ.get('SAP_USERNAME'),
        password=os.environ.get('SAP_PASSWORD'),
        database='PPG_2',
        as_dict=True
    )  

    cursor = cnx.cursor()

    return cursor, cnx

#close connection to pflowDB
def close_connection(cursor, cnx):
    cursor.close()
    cnx.close()
    return

def run_PPGLocationQuery():
    cursor, cnx = connect_to_ppgDB()
    
    query = (
        """
        SELECT 
            [PPG_2].[dbo].[Storageunit].StorageunitName, 
            [PPG_2].[dbo].[Carrier].CarrierName, 
            [PPG_2].[dbo].[Shelf].ShelfName, 
            [PPG_2].[dbo].[Location].LocationName, 
            [PPG_2].[dbo].[Bin].BinName, 
            [PPG_2].[dbo].[LocContentbreakdown].Qualification,
            [PPG_2].[dbo].[Materialbase].MaterialName As 'ItemCode',
            [PPG_2].[dbo].[Materialbase].GroupName As 'ItemType',
            [PPG_2].[dbo].[Materialbase].GroupCode As 'ItemTypeCode',
            [PPG_2].[dbo].[Materialbase].League,
            [PPG_2].[dbo].[Materialbase].Team,
            [PPG_2].[dbo].[Materialbase].Info1,
            [PPG_2].[dbo].[LocContentbreakdown].QuantityCurrent, 
            [PPG_2].[dbo].[LocContent].Countdate,
            [PPG_2].[dbo].[LocContent].Storagedate, 
            [PPG_2].[dbo].[LocContentbreakdown].Putdate, 
            [PPG_2].[dbo].[Materialbase].MaterialId,
            [PPG_2].[dbo].[Location].LocationId, 
            [PPG_2].[dbo].[LocContent].LocContentId, 
            [PPG_2].[dbo].[LocContentbreakdown].LocContentbreakdownId

        FROM 
            [PPG_2].[dbo].[Location] WITH (NOLOCK)
        LEFT JOIN 
            [PPG_2].[dbo].[Shelf] WITH (NOLOCK) ON [PPG_2].[dbo].[Shelf].ShelfId = [PPG_2].[dbo].[Location].ShelfId 
        LEFT JOIN 
            [PPG_2].[dbo].[Carrier] WITH (NOLOCK) ON [PPG_2].[dbo].[Carrier].CarrierId = [PPG_2].[dbo].[Shelf].CarrierId 
        LEFT JOIN 
            [PPG_2].[dbo].[Storageunit] WITH (NOLOCK) ON [PPG_2].[dbo].[Storageunit].StorageunitId = [PPG_2].[dbo].[Carrier].StorageunitId --AND [PPG_2].[dbo].[Storageunit].WarehouseId  =  @PARAM0 
        LEFT JOIN 
            [PPG_2].[dbo].[Bin] WITH (NOLOCK) ON [PPG_2].[dbo].[Bin].BinId = [PPG_2].[dbo].[Location].BinId 
        LEFT JOIN 
            [PPG_2].[dbo].[Zone] WITH (NOLOCK) ON [PPG_2].[dbo].[Zone].ZoneId = [PPG_2].[dbo].[Location].ZoneId 
        LEFT JOIN 
            [PPG_2].[dbo].[LocContent] WITH (NOLOCK) ON [PPG_2].[dbo].[LocContent].LocationId = [PPG_2].[dbo].[Location].LocationId 
        LEFT JOIN 
            [PPG_2].[dbo].[LocContentbreakdown] WITH (NOLOCK) ON [PPG_2].[dbo].[LocContentbreakdown].LocContentId = [PPG_2].[dbo].[LocContent].LocContentId 
        LEFT JOIN 
            [PPG_2].[dbo].[Materialbase] WITH (NOLOCK) ON [PPG_2].[dbo].[Materialbase].MaterialId = [PPG_2].[dbo].[LocContent].MaterialId 
        LEFT JOIN 
            [PPG_2].[dbo].[Materialstoragerule] WITH (NOLOCK) ON [PPG_2].[dbo].[Materialstoragerule].MaterialId = [PPG_2].[dbo].[LocContent].MaterialId AND [PPG_2].[dbo].[Materialstoragerule].BinId  =  [PPG_2].[dbo].[Location].BinId 
        ORDER BY  
            [PPG_2].[dbo].[Storageunit].StorageunitName ASC, 
            [PPG_2].[dbo].[Carrier].CarrierName ASC, 
            [PPG_2].[dbo].[Shelf].ShelfName ASC, 
            [PPG_2].[dbo].[Location].LocationName ASC
            
        """
    )

    cursor.execute(query, )

    #move everything from cursor to a pandas dataframe
    df = pd.DataFrame(cursor.fetchall())

    close_connection(cursor, cnx)

    return df

def get_transactions_df(
    api_base_url: Optional[str] = None,  # Kept for backward compatibility
    access_token: Optional[str] = None,  # Kept for backward compatibility
    order_name: Optional[str] = None,
    transaction_type: Optional[str] = None,
    storageUnitName: Optional[str] = None,
    page_size: int = 1000,  # Not used with direct SQL, but kept for compatibility
    creation_date_from: Optional[str] = None,
    creation_date_to: Optional[str] = None,
    debug: bool = False,
    **kwargs) -> pd.DataFrame:
    """
    Get transactions directly from the database using SQL.
    Replaces the original ItemPath API-based function.
    
    Args remain the same for backward compatibility with the rest of your code.
    
    Returns:
        pandas DataFrame containing transaction data
    """
    try:
        cursor, cnx = connect_to_ppgDB()
        
        # Build the base query
        query = """
        SELECT 
            [History].HistoryId as id,
            CONVERT(VARCHAR(23), [History].Creationdate, 126) as creationDate,
            [History].Type as type,
            [History].WorkOrderName as orderName,
            [History].UserName as userName,
            
            -- Use PickLocationName as the primary location, falling back to PutLocationName
            CASE 
                WHEN [History].PickLocationName IS NOT NULL AND [History].PickLocationName <> '' 
                THEN [History].PickLocationName 
                ELSE [History].PutLocationName 
            END as locationName,
            
            -- Use PickStorageunitName as the primary storage unit, falling back to PutStorageunitName
            CASE 
                WHEN [History].PickStorageunitName IS NOT NULL AND [History].PickStorageunitName <> '' 
                THEN [History].PickStorageunitName 
                ELSE [History].PutStorageunitName 
            END as storageUnitName,
            
            [History].MaterialName as materialName,
            [History].Qualification as qualification,
            [History].QuantityRequested as quantityExpected,
            [History].QuantityConfirmed as quantityConfirmed,
            [History].QuantityDeviated as quantityDeviated,
            [History].MotiveType as motiveType,
            [History].GroupName As groupName,
            [History].GroupCode As groupCode
        FROM 
            [PPG_2].[dbo].[History] WITH (NOLOCK)
        WHERE 
            1=1
        """
        
        # Create a list to hold parameter values
        params = []
        
        # Add MotiveType filter (exclude motive 5 as in original API)
        query += " AND [History].MotiveType != 5"
        
        # Add order_name filter if provided
        if order_name:
            query += " AND [History].WorkOrderName = %s"
            params.append(order_name)
        
        # Parse transaction_type filter from [or]# format
        if transaction_type:
            if "[or]" in transaction_type:
                # Extract numbers from format like "[or]2;4;6;"
                type_values = [int(t) for t in transaction_type.replace("[or]", "").split(";") if t and t.isdigit()]
                
                if type_values:
                    placeholders = ", ".join(["%s"] * len(type_values))
                    query += f" AND [History].Type IN ({placeholders})"
                    params.extend(type_values)
            else:
                # Handle plain number case
                query += " AND [History].Type = %s"
                params.append(int(transaction_type))
        
        # Parse storageUnitName filter from [or]# format
        if storageUnitName:
            if "[or]" in storageUnitName:
                # Extract values from format like "[or]G1;G2;G3;G4"
                storage_units = [su for su in storageUnitName.replace("[or]", "").split(";") if su]
                
                if storage_units:
                    storage_conditions = []
                    
                    for _ in storage_units:
                        storage_conditions.append("([History].PickStorageunitName = %s OR [History].PutStorageunitName = %s)")
                        # Add each storage unit twice (for pick and put)
                        params.extend([_, _])
                    
                    query += " AND (" + " OR ".join(storage_conditions) + ")"
            else:
                # Handle plain string case
                query += " AND ([History].PickStorageunitName = %s OR [History].PutStorageunitName = %s)"
                params.extend([storageUnitName, storageUnitName])
        
        # Add date range filters - use the database to handle date conversion for efficiency
        if creation_date_from:
            query += " AND [History].Creationdate >= %s"
            params.append(creation_date_from)
        
        if creation_date_to:
            query += " AND [History].Creationdate <= %s"
            params.append(creation_date_to)
        
        # Handle any additional filters provided in kwargs
        if 'userName' in kwargs:
            query += " AND [History].UserName = %s"
            params.append(kwargs['userName'])

        query += "AND [History].UserName <> 'VLM'"

        if 'materialName' in kwargs:
            query += " AND [History].MaterialName = %s"
            params.append(kwargs['materialName'])
        
        # Add sorting (newest first)
        query += " ORDER BY [History].Creationdate DESC"
        
        # Debug output
        if debug:
            logging.debug(f"SQL Query: {query}")
            logging.debug(f"Parameters: {params}")
            
        # Execute the query
        cursor.execute(query, tuple(params))
        
        # Create DataFrame from results
        df = pd.DataFrame(cursor.fetchall())
        
        # Close database connection
        close_connection(cursor, cnx)
        
        if debug:
            logging.debug(f"Retrieved {len(df)} records from database")
            
        # Not converting creationDate to datetime here - keeping as string for easier filtering
        return df
        
    except Exception as e:
        logging.error(f"Error in get_transactions_df: {str(e)}")
        if debug:
            logging.debug(f"Query: {query}")
            logging.debug(f"Params: {params}")
        
        # Make sure to close the connection even if there's an error
        try:
            close_connection(cursor, cnx)
        except:
            pass
            
        # Return empty DataFrame on error
        return pd.DataFrame()
    
def get_orders_df(
    api_base_url: Optional[str] = None,  # Kept for backward compatibility
    access_token: Optional[str] = None,  # Kept for backward compatibility
    order_name: Optional[str] = None,
    direction_type: Optional[str] = None,
    warehouse: Optional[str] = None,
    priority: Optional[str] = None,
    deadline_from: Optional[str] = None,
    deadline_to: Optional[str] = None,
    creation_date_from: Optional[str] = None,
    creation_date_to: Optional[str] = None,
    order_status_type: Optional[str] = None,
    special_incomplete: Optional[bool] = None,
    page_size: int = 1000,  # Not used with direct SQL, but kept for compatibility
    debug: bool = False,
    **kwargs) -> pd.DataFrame:
    """
    Get orders directly from the database using SQL.
    Similar to get_transactions_df but for Master Orders.
    
    Args remain the same pattern for consistency and backward compatibility.
    
    Returns:
        pandas DataFrame containing order data
    """
    try:
        cursor, cnx = connect_to_ppgDB()
        
        # Build the base query
        query = """
        SELECT 
            [Masterorder].MasterorderName,
            [Masterorder].DirectionType,
            [Masterorder].Warehouse,
            [Masterorder].Priority,
            CONVERT(VARCHAR(23), [Masterorder].Deadline, 126) as Deadline,
            CONVERT(VARCHAR(23), [Masterorder].Creationdate, 126) as Creationdate,
            [Masterorder].OrderstatusType,
            [Masterorder].SpecialIncomplete
        FROM 
            [PPG_2].[dbo].[Masterorder] WITH (NOLOCK)
        WHERE 
            1=1
        """
        
        # Create a list to hold parameter values
        params = []
        
        # Add order_name filter if provided
        if order_name:
            query += " AND [Masterorder].MasterorderName = %s"
            params.append(order_name)
        
        # Add direction_type filter if provided
        if direction_type:
            if "[or]" in direction_type:
                # Extract values from format like "[or]1;2;3;"
                type_values = [int(t) for t in direction_type.replace("[or]", "").split(";") if t and t.isdigit()]
                
                if type_values:
                    placeholders = ", ".join(["%s"] * len(type_values))
                    query += f" AND [Masterorder].DirectionType IN ({placeholders})"
                    params.extend(type_values)
            else:
                # Handle plain number case
                query += " AND [Masterorder].DirectionType = %s"
                params.append(int(direction_type))
        
        # Add warehouse filter if provided
        if warehouse:
            if "[or]" in warehouse:
                # Extract values from format like "[or]WH1;WH2;WH3;"
                warehouse_values = [wh for wh in warehouse.replace("[or]", "").split(";") if wh]
                
                if warehouse_values:
                    placeholders = ", ".join(["%s"] * len(warehouse_values))
                    query += f" AND [Masterorder].Warehouse IN ({placeholders})"
                    params.extend(warehouse_values)
            else:
                query += " AND [Masterorder].Warehouse = %s"
                params.append(warehouse)
        
        # Add priority filter if provided
        if priority:
            if "[or]" in priority:
                # Extract values from format like "[or]1;2;3;"
                priority_values = [int(p) for p in priority.replace("[or]", "").split(";") if p and p.isdigit()]
                
                if priority_values:
                    placeholders = ", ".join(["%s"] * len(priority_values))
                    query += f" AND [Masterorder].Priority IN ({placeholders})"
                    params.extend(priority_values)
            else:
                # Handle plain number case
                query += " AND [Masterorder].Priority = %s"
                params.append(int(priority))
        
        # Add deadline range filters
        if deadline_from:
            query += " AND [Masterorder].Deadline >= %s"
            params.append(deadline_from)
        
        if deadline_to:
            query += " AND [Masterorder].Deadline <= %s"
            params.append(deadline_to)
        
        # Add creation date range filters
        if creation_date_from:
            query += " AND [Masterorder].Creationdate >= %s"
            params.append(creation_date_from)
        
        if creation_date_to:
            query += " AND [Masterorder].Creationdate <= %s"
            params.append(creation_date_to)
        
        # Add order_status_type filter if provided
        if order_status_type:
            if "[or]" in order_status_type:
                # Extract values from format like "[or]1;2;3;"
                status_values = [int(s) for s in order_status_type.replace("[or]", "").split(";") if s and s.isdigit()]
                
                if status_values:
                    placeholders = ", ".join(["%s"] * len(status_values))
                    query += f" AND [Masterorder].OrderstatusType IN ({placeholders})"
                    params.extend(status_values)
            else:
                # Handle plain number case
                query += " AND [Masterorder].OrderstatusType = %s"
                params.append(int(order_status_type))
        
        # Add special_incomplete filter if provided
        if special_incomplete is not None:
            query += " AND [Masterorder].SpecialIncomplete = %s"
            params.append(1 if special_incomplete else 0)
        
        # Handle any additional filters provided in kwargs
        for key, value in kwargs.items():
            if key in ["MasterorderName", "DirectionType", "Warehouse", "Priority", 
                      "Deadline", "Creationdate", "OrderstatusType", "SpecialIncomplete"]:
                query += f" AND [Masterorder].{key} = %s"
                params.append(value)
        
        # Add sorting (newest first by creation date)
        query += " ORDER BY [Masterorder].Creationdate DESC"
        
        # Debug output
        if debug:
            logging.debug(f"SQL Query: {query}")
            logging.debug(f"Parameters: {params}")
            
        # Execute the query
        cursor.execute(query, tuple(params))
        
        # Create DataFrame from results
        df = pd.DataFrame(cursor.fetchall())
        
        # Close database connection
        close_connection(cursor, cnx)
        
        if debug:
            logging.debug(f"Retrieved {len(df)} records from database")
            
        return df
        
    except Exception as e:
        logging.error(f"Error in get_orders_df: {str(e)}")
        if debug:
            logging.debug(f"Query: {query}")
            logging.debug(f"Params: {params}")
        
        # Make sure to close the connection even if there's an error
        try:
            close_connection(cursor, cnx)
        except:
            pass
            
        # Return empty DataFrame on error
        return pd.DataFrame()