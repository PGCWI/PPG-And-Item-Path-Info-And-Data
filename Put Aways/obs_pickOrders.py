import logging, requests, time, os, pandas as pd    
from typing import Dict, Optional, List
from dotenv import load_dotenv
from enum import Enum
from putAway_itemPathHelpers import get_materials, get_material_properties, get_storage_rules, update_material, create_storage_rule, get_bins
from ppgAutoBatchAndAllocating import get_batches, get_work_order_lines, update_work_order_line, get_orders, create_batch
from pullList import read_specialPickList

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Direction(Enum):
    PICK = 2
    PUT = 1

class Item:
    def __init__(self, code: str, qualification: str, quantity: int, direction: Direction, zone: str, materialID:str, location: str = None):
        self.code = code
        self.qualification = qualification
        self.quantity = quantity
        self.direction = direction
        self.orderID = None
        self.MaterialID = materialID
        self.zone = zone
        self.location = location

    def update_orderID(self, orderID):
        self.orderID = orderID
        return

    def get_zone(self):
        return self.zone

    def get_orderID(self):
        return self.orderID
    
    def get_direction(self):
        return self.direction.value
    
    def get_convertedOrderLine(self):
        return {"materialId": self.MaterialID, "quantity": self.quantity, "qualification": self.qualification}


    def __repr__(self):
        return f"Item(code='{self.code}', qualification='{self.qualification}', quantity={self.quantity}, direction={self.direction.value})"

class Batch:
    def __init__(self, name: str, source_skid: str, qualification: str):
        self.name = name
        self.source_skidName = source_skid
        self.qualification = qualification
        self.items: List[Item] = []

    def add_item(self, item: Item):
        self.items.append(item)
        
        return self.items[-1]


    def get_name(self):
        return self.name
    
    def get_source_skid(self):
        return self.source_skidName

    def get_items(self) -> List[Item]:
        return self.items

    def get_batchDetails(self):
        #output a nice print statement with batch info (except for items)
        return f"Batch Name: {self.name}, Source Skid: {self.source_skidName}, Qualification: {self.qualification}"

    def __repr__(self):
        return f"Batch(items={self.items})"

def match_bin_ids(df_bins, bins):
    """
    Match bin names from array to dataframe and return corresponding IDs
    
    Args:
        df_bins (pd.DataFrame): DataFrame containing bin data with 'name' and 'id' columns
        bins (list): List of bin names to match
        
    Returns:
        list: Matched IDs or None for non-matches
    """
    # Convert to dict for O(1) lookup
    bin_dict = df_bins.set_index('name')['id'].to_dict()
    
    # Map each bin to its ID or None if not found
    return [bin_dict.get(bin) for bin in bins]

def prepare_binSizes():
    dfBins = pd.DataFrame(get_bins()['bins'])
    bins = ["GS","GL","GX"]
    binSizeIsBig = [True,True,False]
    binIDs = match_bin_ids(dfBins, bins)

    return binIDs, binSizeIsBig

def determineMaterialProperties(league, qualification, material, inWHS=True):
    dflt_name = "Default"
    name = dflt_name
    maxInBin = 0
    maxInLowQTYBin = 0 

    G1_SpecialCodeList = ["0"]
    G2_SpecialCodeList = ["0"]

    itemCode = material['materials'][0]['name']
    itemDesc = material['materials'][0]['Info1']

    #if in WHS:
    if inWHS:
        if qualification == '14':
            #special case for hockey if On-Ice is in material[]
            if itemCode.upper() in G1_SpecialCodeList:
                name = "Fanatics Jerseys - USA - G1 Special Events"
            elif itemCode.upper() in G2_SpecialCodeList:
                name = "Fanatics Jerseys - USA - G2 Special Events"
            elif league in ["NHL", "PWHL", "IIHF"]:
                if "On-Ice".upper() in itemDesc.upper():
                    name = "Fanatics Jerseys - USA - HOC ON-ICE"
                else:
                    name = "Fanatics Jerseys - USA - HOC"
            elif league in ["NBA","WNBA"]:
                name = "Fanatics Jerseys - USA - BSK"
            elif league in ["NFL"]:
                name = "Fanatics Jerseys - USA - FTBL"
            else:
                name= None
        elif qualification == '08':
            if league in ["NHL"]:
                name = "Fanatics Jerseys - CAD - NHL"
            else:
                name = "Fanacics Jerseys - CAD - Other"

            name = None 
    else:
        #if not in WHS then do skid racking
        name = dflt_name
        name = None #error for now
        
    if league in ["NHL", "PWHL", "IIHF"]:
        maxInBin = 24
        maxInLowQTYBin = 3
    else:
        maxInBin = 48
        maxInLowQTYBin = 5
    return name, maxInBin, maxInLowQTYBin


def prepareCodeForPutOrder_ARCHIVED(itemCode, qualification, binIDs, binSizes):
    # Get the material details
    material = get_materials(itemCode)
    if material is None:
        return None, None
    materialId = material['materials'][0]['id']
    material_league = material['materials'][0]['League']
    material_propertyID = material['materials'][0]['materialPropertyId']

    #item properties
    zoneName, maxBigBin, maxLowQTYBin = determineMaterialProperties(league=material_league, qualification=qualification, material=material)

    #get the new material storage rule
    newMaterialPropertyID = get_material_properties(name=zoneName)
    newMaterialPropertyID = newMaterialPropertyID['material_properties'][0]['id']

    #check if the material_propertyID = newMaterialPropertyID - if it does not then
    if material_propertyID != newMaterialPropertyID:
        material_propertyID = update_material(material_id=materialId, newPropertyID=newMaterialPropertyID)
        
    #get the storage rules for the material 
    storage_rules = get_storage_rules(materialID=materialId)

    #check if 'storage_rules'
    try:
        storage_rules = storage_rules['storage_rules']
    except:
        storage_rules = []

    #convert storage_rules to a data frame
    dfStorageRules = pd.DataFrame(storage_rules)

    #only show dfStorageRules where materialID = materialID
    dfStorageRules = dfStorageRules[dfStorageRules['materialId'] == materialId]

    #loop through each binID
    i = 0
    for bin in binIDs:
        
        binInDF = dfStorageRules[dfStorageRules['binId'] == bin]

        if len(binInDF) == 0:
            #create storage rule for the material
            
            maxStock = maxBigBin if binSizes[i] else maxLowQTYBin
            minStock = 1 #maxLowQTYBin if binSizes[i] else 1
            tempDesc = zoneName + " - " + "Normal" if binSizes[i] else zoneName + " - " + "Low QTY"
            z = create_storage_rule(material_id=materialId,bin_id=bin, min_stock=minStock, max_stock=maxStock,required_capacity=100,description=tempDesc)
            time.sleep(5)
        i += 1

    return materialId, zoneName


def prepareCodeForPutOrder(itemCode, qualification, binIDs, binSizes):
    # Get the material details
    material = get_materials(itemCode)
    if material is None:
        return None, None
    materialId = material['materials'][0]['id']
    material_league = material['materials'][0]['League']
    material_propertyID = material['materials'][0]['materialPropertyId']

    #item properties
    zoneName, maxBigBin, maxLowQTYBin = determineMaterialProperties(league=material_league, qualification=qualification, material=material)

    #get the storage rules for the material 
    storage_rules = get_storage_rules(materialID=materialId)

    #check if 'storage_rules'
    try:
        storage_rules = storage_rules['storage_rules']
    except:
        storage_rules = []

    #convert storage_rules to a data frame
    dfStorageRules = pd.DataFrame(storage_rules)

    #check if dfStorageRules is empty
    if not dfStorageRules.empty:
        #only show dfStorageRules where materialID = materialID
        dfStorageRules = dfStorageRules[dfStorageRules['materialId'] == materialId]

    #loop through each binID
    i = 0
    for bin in binIDs:
        
        #check if dfStorageRules is empty
        if dfStorageRules.empty:
            binInDF = pd.DataFrame()
        else:
            binInDF = dfStorageRules[dfStorageRules['binId'] == bin]

        if len(binInDF) == 0:
            #create storage rule for the material
            
            maxStock = maxBigBin if binSizes[i] else maxLowQTYBin
            minStock = 1 #maxLowQTYBin if binSizes[i] else 1
            tempDesc = zoneName + " - " + "Normal" if binSizes[i] else zoneName + " - " + "Low QTY"
            z = create_storage_rule(material_id=materialId,bin_id=bin, min_stock=minStock, max_stock=maxStock,required_capacity=100,description=tempDesc)
            time.sleep(5)
        i += 1

    #get the new material storage rule
    newMaterialPropertyID = get_material_properties(name=zoneName)
    newMaterialPropertyID = newMaterialPropertyID['material_properties'][0]['id']

    #check if the material_propertyID = newMaterialPropertyID - if it does not then
    if material_propertyID != newMaterialPropertyID:
        material_propertyID = update_material(material_id=materialId, newPropertyID=newMaterialPropertyID)
        
    return materialId, zoneName

def create_pickorder(name: str, itm: Item, skidName: str, debug: bool = False) -> Optional[Dict]:
    """
    Creates a new order in the ItemPath API.
    
    Args:
        name (str): Name of the order
        order_lines (List[Dict]): List of order line items containing material details
        debug (bool): Enable detailed logging for troubleshooting
        
    Returns:
        dict: Created order details as Python dictionary, or None if error occurs
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    url = f"{base_url}/api/orders"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    pickLocation = ""
    if skidName == "" or skidName == None:
        pickLocation = itm.location
    else:
        pickLocation = skidName

    order_lines = [itm.get_convertedOrderLine()]
    data = {
        "name": name,
        "directionType": itm.get_direction(),
        "order_lines": order_lines,
        "allocate": True,
        "locations": [{"name": pickLocation}]
        
    }

    try:
        response = requests.post(url, headers=headers, json=data)
            
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
        logger.error(f"Response Content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating order: {e}")
        return None
    
def create_putorder(name: str, itm: Item, zone: str, debug: bool = False) -> Optional[Dict]:
    """
    Creates a new order in the ItemPath API.
    
    Args:
        name (str): Name of the order
        order_lines (List[Dict]): List of order line items containing material details
        debug (bool): Enable detailed logging for troubleshooting
        
    Returns:
        dict: Created order details as Python dictionary, or None if error occurs
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    url = f"{base_url}/api/orders"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    order_lines = [itm.get_convertedOrderLine()]

    data = {
        "name": name,
        "directionType": itm.get_direction(),
        "order_lines": order_lines,
        "allocate": True,
        "zones": [{"name":itm.get_zone()}]
    }

    
    try:
        response = requests.post(url, headers=headers, json=data)

        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
        logger.error(f"Response Content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating order: {e}")
        return None

def getAndCleanMaterialID(itemCode):
    material = get_materials(itemCode)
    if material is None:
        return None
    return material['materials'][0]['id']

def create_skidPullBatchObj(skidName: str, skidQualification: str, itemCodes: List[str], qty: List[int], binIDs: List[int], binSizes: List[bool]):
    #create the batch object
    batchName =  time.strftime("%y%m%d") + "." + skidName + ".SkidPull"  
    batch = Batch(name=batchName, source_skid=skidName, qualification=skidQualification)
    
    pickItems = []
    putItems = []
    pickPutItems = []
    #iterate through each item code, prepare it, pull the zone
    i = 0
    while i < len(itemCodes):
        materialID, zone = prepareCodeForPutOrder(itemCodes[i], skidQualification, binIDs, binSizes)
        
        pickItems.append(Item(code=itemCodes[i], qualification=skidQualification, quantity=qty[i], direction=Direction.PICK, zone=zone, materialID = materialID))
        putItems.append(Item(code=itemCodes[i], qualification=skidQualification, quantity=qty[i], direction=Direction.PUT, zone=zone, materialID = materialID))
        
        pickPutItems.append(Item(code=itemCodes[i], qualification=skidQualification, quantity=qty[i], direction=Direction.PICK, zone=zone, materialID = materialID))
        pickPutItems.append(Item(code=itemCodes[i], qualification=skidQualification, quantity=qty[i], direction=Direction.PUT, zone=zone, materialID = materialID))
        
        i += 1

    orderArr = pickItems + putItems
    #orderArr = pickPutItems
    
    i = 0
    while i < len(orderArr):
        batch.add_item(orderArr[i])
        i += 1

    return batch

def create_whsPickBatchObj(batchName: str, qual: str, locations: List[str], itemCodes: List[str], qty: List[int], binIDs: List[int], binSizes: List[bool]):
    batch = Batch(name=batchName, source_skid="", qualification=qual)

    pickItems = []
    i = 0
    while i < len(itemCodes):
        materialID, zone = prepareCodeForPutOrder(itemCodes[i], qual, binIDs, binSizes)
        pickItems.append(Item(code=itemCodes[i], qualification=qual, quantity=qty[i], direction=Direction.PICK, zone=zone, materialID = materialID, location = locations[i]))

        i += 1

    orderArr = pickItems

    i = 0
    while i < len(orderArr):
        batch.add_item(orderArr[i])
        i += 1


    return batch

def moveBatchObjToPPG(batch: Batch):
    batchName = batch.get_name()

    #create the pick and put orders in PPG
    i = 0 
    while i < len(batch.items):
        #create an order for the item
        print(batchName + "." +str(i+1))

        if batch.items[i].get_direction() == Direction.PICK.value:
            #create pick order
            #check if location is null
            
            z = create_pickorder(name=batchName + "." + str(i+1), itm=batch.items[i], skidName=batch.get_source_skid())
            print(z)
            if z is not None:
                batch.items[i].update_orderID(z['order']['id'])
        elif batch.items[i].get_direction() == Direction.PUT.value:
            #create put order
            z = create_putorder(name=batchName+ "." + str(i+1), itm=batch.items[i], zone=batch.items[i].get_zone())
            print(z)
            if z is not None:
                batch.items[i].update_orderID(z['order']['id'])
        i += 1

    batch_id = get_batches(batchName)
    if not batch_id:
        x = create_batch(batchName)
        print(x)

    time.sleep(1)
    batch_id = get_batches(batchName)
    if batch_id is None:
        #sleep for 3 seconds
        time.sleep(3)
        batch_id = get_batches(batchName)
        time.sleep(1)
        if batch_id is None:
            create_batch(batchName)
            time.sleep(1)
        batch_id = get_batches(batchName)

    if batch_id is None:
        return False
    
    if batch_id:
        k = 1
        for items in batch.items:
            work_order_lines = get_work_order_lines(items.get_orderID())
            print(work_order_lines)
            if work_order_lines:
                for line in work_order_lines:
                    update_work_order_line(line['id'], batch_id, handling_rank=k)
            k += 1

    return True

def main():

    print("hello world")
    #initial one time preparation
    binIDs, binSizes = prepare_binSizes()
    
    df = read_specialPickList()

    #filter oyt all rows where Added is not none
    print(df)

    #output df to csv
    df.to_csv('outputBatches.csv')

    #convert this df and aggregate it and concatenate qualifications, batch names and whs
    batchDf = df.groupby(['BatchName']).agg({'ItemCode': lambda x: ', '.join(x), 'QTY': lambda x: ', '.join(map(str, x)), 'Location': lambda x: ', '.join(x), 'Qualification': lambda x: ', '.join(x)}).reset_index()

    #iterate through each row in the data frame
    for index, row in batchDf.iterrows():
        #skid and batch preparation info
        batchName = row['BatchName']
        qual = row['Qualification']

        #split qual and return uniques
        qual = list(set(qual.split(', ')))

        #make qual = to a string of qual
        qual = ', '.join(qual)

        #item codes is a comma elimited string
        itemCodes = row['ItemCode'].split(', ')
        qty = row['QTY'].split(', ')
        locations = row['Location'].split(', ')
        print(locations)

        #ocnvert qty into a list of integers
        qty = [int(x) for x in qty]

        batch = create_whsPickBatchObj(batchName, qual, locations, itemCodes, qty, binIDs, binSizes)
        moveBatchObjToPPG(batch)

    return 

    
if __name__ == "__main__":
    main()