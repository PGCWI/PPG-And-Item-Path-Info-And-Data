import pandas as pd
import requests
from typing import Dict, Optional, List
import os
from dotenv import load_dotenv
import logging
import time
from ppgAutoBatchAndAllocating import get_batches, get_work_order_lines, update_work_order_line, get_orders, create_batch
from enum import Enum
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Direction(Enum):
    PICK = 2
    PUT = 1

class Item:
    def __init__(self, code: str, qualification: str, quantity: int, direction: Direction, zone: str, note:str):
        self.code = code
        self.qualification = qualification
        self.quantity = quantity
        self.direction = direction
        self.orderID = None
        self.MaterialID = None
        self.zone = zone
        self.note = note

    def update_note(self, note):
        self.note = note
        return

    def update_zone(self,zone):
        self.zone = zone
        return

    def update_orderID(self, orderID):
        self.orderID = orderID
        return

    def updateMaterialID(self, materialID):
        self.MaterialID = materialID
        return
    
    def get_note(self):
        return self.note
    
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
    def __init__(self, name):
        self.items: List[Item] = []
        self.name: name

    def add_item(self, item: Item):
        self.items.append(item)

    def get_name(self):
        return self.name

    def get_items(self) -> List[Item]:
        return self.items

    def __repr__(self):
        return f"Batch(items={self.items})"

def get_materials(name: str) -> Optional[Dict]:
    """
    Fetches materials from the API with specified info1 parameter.
    
    Args:
        info1 (str): The info1 parameter to filter materials.
        
    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    params = {"name": name}
    url = f"{base_url}/api/materials"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching materials: {e}")
        return None

def determineMaterialProperties(league, qualification, inWHS=True):
    dflt_name = "Default"
    name = dflt_name
    maxInBin = 0
    maxInLowQTYBin = 0 
    #if in WHS:
    if inWHS:
        if qualification == '14':
            #if qualification is 14 then G1 or G2
            if league in ["NHL", "PWHL",  "IIHF"]:
                name = "Fanatics Jerseys - USA - HOC"
                maxInBin = 24
            elif league in ["NBA","WNBA"]:
                name = "Fanatics Jerseys - USA - BSK"
            elif league in ["NFL"]:
                name = "Fanatics Jerseys - USA - FTBL"
            else:
                name= None
                #not error handling rn
        elif qualification == '08':
            name = "Fanatics Jerseys - CAD - "

            if league in ["NHL"]:
                name = "Fanatics Jerseys - CAD - NHL"
            else:
                name = "Fanacics Jerseys - CAD - Other"

            name = None #not doing CAD rn

            #if qualification is 08 then G3 or G4

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

def get_bins():
    """
    Fetches all bins from the ItemPath API.

    Returns:
        dict: The JSON response containing bins data, or None if an error occurs.
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    url = f"{base_url}/api/bins"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching bins: {e}")
        return None
    
def get_material_properties(name: str = "default") -> Optional[Dict]:
    """
    Fetches material properties from the API with specified name parameter.
    
    Args:
        name (str): Name of material properties to retrieve. Defaults to "basic".
    
    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    url = f"{base_url}/api/material_properties"
    
    params = {
        "name": name
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching material properties: {e}")
        return None

def update_material(material_id: str, newPropertyID: str) -> Optional[Dict]:
    """
    Updates material details in the ItemPath API.
    
    Args:
        material_id (str): ID of the material to update
        updates (Dict[str, str]): Dictionary containing fields to update
    
    Returns:
        dict: API response as dictionary, None if error occurs
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in .env file")

    url = f"{base_url}/api/materials/{material_id}"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    data = {
        "materialPropertyId": newPropertyID
    }

    try:
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error updating material: {e}")
        return None
    
def get_storage_rules(materialID) -> Optional[Dict]:
    """
    Fetches storage rules from the ItemPath API.
    
    Returns:
        dict: The JSON response from the API as a Python dictionary containing storage rules,
              or None if an error occurs.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")  # e.g., http://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    # Construct the full URL
    url = f"{base_url}/api/storage_rules"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    params = {
        "limit": 15,
        "materialId": materialID
    }

    try:

        # Make the GET request
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        
        return response.json()  # Return the JSON response as a dictionary
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching storage rules: {e}")
        return None
    
def create_storage_rule(
    material_id: str,
    bin_id: str,
    min_stock: int,
    max_stock: int,
    required_capacity: int,
    is_default_bin: bool = False,
    description: str = ""
) -> Optional[Dict]:
    """
    Creates a storage rule via the ItemPath API.
    
    Args:
        material_id (str): UUID of the material
        bin_id (str): UUID of the bin
        min_stock (int): Minimum stock per bin
        max_stock (int): Maximum stock per bin
        required_capacity (int): Required capacity
        pack_size (int): Pack size
        pack_size_usage_type (str): Pack size usage type
        min_locations (int): Minimum number of locations
        max_locations (int): Maximum number of locations
        min_stock_per_location (int): Minimum stock per location
        is_default_bin (bool): Whether this is the default bin
        always_use_new_location (bool): Whether to always use new location
        description (str): Description of the storage rule
    
    Returns:
        dict: API response as dictionary or None if error occurs
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in .env file")

    url = f"{base_url}/api/storage_rules"
    
    payload = {
        "materialId": material_id,
        "binId": bin_id,
        "description": description,
        "isDefaultBin": is_default_bin,
        "minStockPerBin": min_stock,
        "maxStockPerBin": max_stock,
        "requiredCapacity": required_capacity
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error creating storage rule: {e}")
        return None

def prepareCodeForPutOrder(itemCode, qualification, binIDs, binSizes):
    #item ID 
    # Get the material ID
    material = get_materials(itemCode)
    #if material is not found, return
    if material is None:
        return    
    materialId = material['materials'][0]['id']
    material_league = material['materials'][0]['League']
    material_propertyID = material['materials'][0]['materialPropertyId']

    #item properties
    zoneName, maxBigBin, maxLowQTYBin = determineMaterialProperties(league=material_league, qualification=qualification)

    #zoneName = "Fanatics Jerseys" # temporary TODELETE

    #get the new material storage rule
    newMaterialPropertyID = get_material_properties(name=zoneName)
    newMaterialPropertyID = newMaterialPropertyID['material_properties'][0]['id']

    #check if the material_propertyID = newMaterialPropertyID - if it does not then
    if material_propertyID != newMaterialPropertyID:
        newMaterial = update_material(material_id=materialId, newPropertyID=newMaterialPropertyID)
        print(newMaterial)
        

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
        
        print(bin)
        binInDF = dfStorageRules[dfStorageRules['binId'] == bin]

        if len(binInDF) == 0:
            #create storage rule for the material
            
            maxStock = maxBigBin if binSizes[i] else maxLowQTYBin
            minStock = 1 #maxLowQTYBin if binSizes[i] else 1
            tempDesc = zoneName + " - " + "Normal" if binSizes[i] else zoneName + " - " + "Low QTY"
            z = create_storage_rule(material_id=materialId,bin_id=bin, min_stock=minStock + 1, max_stock=maxStock,required_capacity=100,description=tempDesc)
        i += 1

    return zoneName
   
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
    

    order_lines = [itm.get_convertedOrderLine()]
    data = {
        "name": name,
        "directionType": itm.get_direction(),
        "order_lines": order_lines,
        "allocate": True,
        "locations": [{"name": skidName}]
        
    }

    if debug:
        logger.info(f"Request URL: {url}")
        logger.info(f"Request Headers: {headers}")
        logger.info(f"Request Data: {data}")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        if debug and not response.ok:
            logger.error(f"Response Status: {response.status_code}")
            logger.error(f"Response Content: {response.text}")
            
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

    print("HII")
    print(zone)

    data = {
        "name": name,
        "directionType": itm.get_direction(),
        "order_lines": order_lines,
        "allocate": True,
        "zones": [{"name":itm.get_zone()}]
    }

    if debug:
        logger.info(f"Request URL: {url}")
        logger.info(f"Request Headers: {headers}")
        logger.info(f"Request Data: {data}")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        if debug and not response.ok:
            logger.error(f"Response Status: {response.status_code}")
            logger.error(f"Response Content: {response.text}")
            
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
        logger.error(f"Response Content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating order: {e}")
        return None




def create_skidPickPullOrder(batch):

    return



def main():
    load_dotenv()
    
    """
    orderName = "241218.Skid3"
    skidName = "SCG-20241021-103"

    codes = ["524656"]
    qty = [16]
    """

    orderName = "241218.Skid2"
    skidName = "SCG-20241211-007"

    codes = ["428593"]
    qty = [58]
    codeNote = [None]


    dfBins = pd.DataFrame(get_bins()['bins'])
    bins = ["GS","GL","GX"]
    binSizeIsBig = [True,True,False]
    binIDs = match_bin_ids(dfBins, bins)
    
    pickItms = []
    putItms = []
    qual = "14"

    i = 0 
    while i < len(codes):
        zone = prepareCodeForPutOrder(itemCode=codes[i], qualification=qual,binIDs=binIDs, binSizes=binSizeIsBig)
        pickItms.append(Item(codes[i], qual, qty[i], Direction.PICK, zone, codeNote[i]))
        putItms.append(Item(codes[i], qual, qty[i], Direction.PUT, zone, codeNote[i]))
        i += 1

    itms = pickItms + putItms

    # Create order and add items
    batch = Batch(name=orderName)

    for itm in itms:
        #add materialID
        materialID = get_materials(name=itm.code)
        itm.updateMaterialID(materialID=materialID["materials"][0]["id"])

        batch.add_item(itm)

    # Access items
    items = batch.get_items()

    orderNames = []
    i = 1
    for item in batch.items:


        #create an order for the item
        print('new order')
        print(orderName + "." +str(i))
        if item.get_direction() == Direction.PICK.value:
            #create pick order
            orderNames.append(orderName + "." + str(i))
            z = create_pickorder(name=orderName + "." +str(i), itm=item, skidName=skidName)
            print(z)
        elif item.get_direction() == Direction.PUT.value:
            #create put order
            orderNames.append(orderName + "." + str(i))
            z = create_putorder(name=orderName + "." +str(i), itm=item)
            print(z)
        i += 1

    print(items)

    batchName = orderName
    
    batch_id = get_batches(batchName)
    if batch_id:
        batch_created = True
    else:
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
        return
    
    print(orderNames)
    orderIDs = []
    for orderNs in orderNames:
        print(orderNs)
        orderIDs.append(get_orders(soNumber=orderNs))

    if batch_id:
        k = 1
        for ids in orderIDs:
            print(ids)
            work_order_lines = get_work_order_lines(ids)
            print(work_order_lines)
            if work_order_lines:
                for line in work_order_lines:
                    update_work_order_line(line['id'], batch_id, handling_rank=k)
            k += 1

    print('batch created')

    return 

if __name__ == "__main__":
    main()