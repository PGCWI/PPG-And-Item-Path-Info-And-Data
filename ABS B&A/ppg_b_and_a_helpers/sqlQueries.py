import os, pymssql, pandas as pd

def connect_to_sapDB():
    cnx = pymssql.connect(
        server='sql-srvr-01.silvercrystal.com',
        user=os.environ.get('SAP_USERNAME'),
        password=os.environ.get('SAP_PASSWORD'),
        database='scs',
        as_dict=True
    )  
    cursor = cnx.cursor()
    return cursor, cnx

def close_connection(cursor, cnx):
    cursor.close()
    cnx.close()
    return

def run_sapBOMQuery(soNums):
    cursor, cnx = connect_to_sapDB()
    
    query = f"""
        SELECT T0.[DocNum],T0.[DocDueDate],T1.[LineNum],T1.[WhsCode], T1.[ItemCode], 
               T1.[Dscription], T1.[Quantity], T1.[FreeTxt], 
               T5.[ItmsGrpCod], T5.[U_PLS_PPG_ITEM], 
               CAST(T6.MaterialId AS VARCHAR(36)) AS MaterialId
        FROM [SCS].[dbo].[ORDR] T0 WITH (NOLOCK)
        INNER JOIN [SCS].[dbo].[RDR1] T1 WITH (NOLOCK) ON T0.[DocEntry] = T1.[DocEntry] 
        INNER JOIN [SCS].[dbo].[OCRD] T2 WITH (NOLOCK) ON T0.[CardCode] = T2.[CardCode] 
        INNER JOIN [SCS].[dbo].[OCRG] T3 WITH (NOLOCK) ON T2.[GroupCode] = T3.[GroupCode] 
        LEFT JOIN [SCS].[dbo].[@PLS_BOM_CUSTOMERS] T4 WITH (NOLOCK) ON T2.[CardCode] = T4.[U_CardCode] 
        INNER JOIN [SCS].[dbo].[OITM] T5 WITH (NOLOCK) ON T1.[ItemCode] = T5.[ItemCode] 
        LEFT JOIN [PPG_2].[dbo].[Materialbase] T6 WITH (NOLOCK) ON T6.MaterialName COLLATE SQL_Latin1_General_CP850_CI_AS = T1.ItemCode
        WHERE T0.[DocNum] in {soNums}
    """
    
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    close_connection(cursor, cnx)
    
    return df

def run_getOpenPPGPickOrders():
    cursor, cnx = connect_to_sapDB()
    
    query = f"""
        SELECT 
            CAST([Masterorder].MasterorderId AS VARCHAR(36)) AS MasterorderId,
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
            [Masterorder].DirectionType = 2
    
    """
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    close_connection(cursor, cnx)
    
    return df

def run_getOpenPPGPickOrders_specific(orderName):
    cursor, cnx = connect_to_sapDB()
    
    query = f"""
        SELECT 
            CAST([Masterorder].MasterorderId AS VARCHAR(36)) AS MasterorderId,
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
            [Masterorder].DirectionType = 2 and [MasterOrder].MasterorderName = '{orderName}'
    
    """
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    close_connection(cursor, cnx)
    
    return df

def run_getOpenPPGBatches_specific(batchName):
    cursor, cnx = connect_to_sapDB()

    query = f"""
    Select CAST([Workorder].WorkorderId AS VARCHAR(36)) As WorkorderId, 
    [Workorder].WorkorderName 
    FROM [PPG_2].[dbo].[Workorder] WITH (NOLOCK)
    WHERE [Workorder].WorkorderName = '{batchName}'
    """

    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    close_connection(cursor, cnx)
    
    return df

def run_getOpenPPGWOLines_specific(order_id):
    cursor, cnx = connect_to_sapDB()

    query = f"""
    Select CAST([Workorderline].WorkorderlineId AS VARCHAR(36)) AS WorkorderlineId , 
    CAST([Workorderline].MasterorderlineId AS VARCHAR(36)) AS MasterorderlineId,
    CAST([Masterorderline].MasterorderId AS VARCHAR(36)) AS MasterorderId
    FROM [PPG_2].[dbo].[Workorderline] WITH (NOLOCK) 
    LEFT OUTER JOIN [PPG_2].[dbo].[Masterorderline] WITH (NOLOCK) ON [PPG_2].[dbo].[Masterorderline].MasterorderlineId = [PPG_2].[dbo].[Workorderline].MasterorderlineId
    WHERE CAST([Masterorderline].MasterorderId AS VARCHAR(36)) = '{order_id}'
    """

    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    close_connection(cursor, cnx)
    
    return df