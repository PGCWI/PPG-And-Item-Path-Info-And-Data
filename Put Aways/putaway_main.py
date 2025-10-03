import pandas as pd
import requests
from typing import Dict, Optional, List
import os
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def get_carriers() -> Optional[Dict]:
    """
    Fetches a list of carriers from the ItemPath API.
    
    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    url = f"{base_url}/api/carriers"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching carriers: {e}")
        return None

def get_storage_units() -> Optional[Dict]:
    """
    Fetches list of all storage units from the ItemPath API.
    
    Returns:
        dict: List of storage units as a Python dictionary, or None if error occurs.
    """
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    url = f"{base_url}/api/storage_units"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching storage units: {e}")
        return None
        


def create_order(
    name: str,
    order_lines: List[Dict],
    debug: bool = False
) -> Optional[Dict]:
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
    
    data = {
        "name": name,
        "directionType": 1,
        "order_lines": order_lines,
        "zones": [{"name": "Fanatics Jerseys"}]
        
    }
    #"carriers": [{"name": "206-D"}]

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
        "limit":1   
    }

    #materialId": materialID

    try:

        # Make the GET request
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        
        return response.json()  # Return the JSON response as a dictionary
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching storage rules: {e}")
        return None

def get_zones() -> Optional[Dict]:
    """
    Fetches a list of zones from the ItemPath API.
    
    Returns:
        dict: The JSON response from the API as a Python dictionary containing zones data,
              or None if an error occurs.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")  # e.g., http://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    # Construct the full URL
    url = f"{base_url}/api/zones"
    
    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        # Make the GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        
        return response.json()  # Return the JSON response as a dictionary
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching zones: {e}")
        return None


def get_material(material_id: str) -> Optional[Dict]:
    """
    Fetches material information from the API for a specific material ID.

    Args:
        material_id (str): The ID of the material to retrieve.

    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")  # e.g., http://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    # Construct the full URL
    url = f"{base_url}/api/materials/{material_id}"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        # Make the GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching material: {e}")
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

def chooseMaterialProperty(league, qualification, qty, inWHS=True):
    dflt_name = "Default"
    name = dflt_name
    maxInBin = 0
    maxInLowQTYBin = 0 
    #if in WHS:
    if inWHS:
        if qualification == '14':
            #if qualification is 14 then G1 or G2
            if league in ["NHL", "PWHL"]:
                name = "Fanatics Jerseys - USA - NHL"
                maxInBin = 24
            elif league in ["NBA","WNBA"]:
                name = "Fanatics Jerseys - USA - BSK"
            elif league in ["NFL"]:
                name = "Fanatics Jerseys - USA - NFL"
            else:
                name = "Fanatics Jerseys - USA - Other"
        elif qualification == '08':
            name = "Fanatics Jerseys - CAD - "

            if league in ["NHL"]:
                name = "Fanatics Jerseys - CAD - NHL"
            else:
                name = "Fanacics Jerseys - CAD - Other"

            #if qualification is 08 then G3 or G4

    else:
        #if not in WHS then do skid racking
        name = dflt_name
        
    if league in ["NHL", "PWHL", "IIHF"]:
        name = "Fanatics Jerseys - USA - NHL"
        maxInBin = 24
        maxInLowQTYBin = 3
    else:
        maxInBin = 48
        maxInLowQTYBin = 5
    return name,

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

def createPutOrder(materialName, qualification, zone):
    # Get the material ID
    material = get_materials(materialName)
    print(material)

    #if material is not found, return
    if material is None:
        return
    
    materialId = material['materials'][0]['id']
    print(materialId)

    print(get_material(materialId))

    materialPropertyID_Default = get_material_properties()
    materialPropertyID_Jersey = get_material_properties(name="Fanatics Jerseys")

    matPropertyID = materialPropertyID_Default['material_properties'][0]['id']
    print(matPropertyID)

    update_material(material_id=materialId, newPropertyID=matPropertyID)

    return
    #Get the material storage rules for that material ID
    storageRules = get_storage_rules(materialId)
    print(storageRules)

    #then update 

    return



def main():
    print("Hello from putaway_main.py")
    load_dotenv()

    itemCode = "541995"

    #Pull Bins
    dfBins = pd.DataFrame(get_bins()['bins'])
    bins = ["GS","GL","GX"]
    binSizeIsBig = [True,True,False]
    binIDs = match_bin_ids(dfBins, bins)
    
    return

    x = get_materials(itemCode)
    print(x)
    print(x['materials'][0]['id'])

    xId = x['materials'][0]['id']
    print(get_storage_rules(materialID=xId))

    return
    
    print(createPutOrder(itemCode, "Fanatics Jerseys"))
    return
    x = get_materials(itemCode)
    print(x)
    print(x['materials'][0]['id'])

    xId = x['materials'][0]['id']


    #create an order
    order_lines = [
        {
            "materialId": xId,
            "quantity": 1,
            "qualification": "33"
        }
    ]

    x = create_order("THISISATEST3", order_lines, True)


    return
    #get all storage units
    x = get_storage_units()
    df = pd.DataFrame(x['storage_units'])
    df.to_csv('dfStorageUnits.csv', index=False)



    return


    #get all carriers
    x = get_carriers()
    print(x)
    #convert x json to a dataframe
    df = pd.DataFrame(x['carriers'])
    print(df)

    df.to_csv('dfCarriers.csv', index=False)
    return


if __name__ == "__main__":
    main()