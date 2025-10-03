import requests
from typing import Dict, Optional, List
import os
from dotenv import load_dotenv
import logging

load_dotenv()

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
        "limit": 10000,
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
    

