import pandas as pd
import numpy as np
import requests
from typing import Dict, Optional, Any, List, Callable
import os 
from dotenv import load_dotenv
import time
from pathlib import Path
from app.env import load_environment
import functools
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

env_config = load_environment()

base_url = env_config['ITEMPATH_URL']
auth_token = env_config['ITEMPATH_APPLICATION_TOKEN']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_robust_session():
    """Create a requests session with connection pooling and timeouts"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=2,  # Low retry count
        backoff_factor=0.3,
        status_forcelist=[502, 503],  # Only retry on server errors, not 504
        allowed_methods=["GET", "PUT", "POST", "DELETE"]
    )
    
    # Configure connection pooling
    adapter = HTTPAdapter(
        pool_connections=20,  # Number of connection pools
        pool_maxsize=50,      # Connections per pool
        max_retries=retry_strategy
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Global session
api_session = create_robust_session()

def smart_retry_on_exception(max_attempts: int = 3, initial_delay: float = 0.5, 
                           max_delay: float = 5.0, backoff_factor: float = 2.0, 
                           exceptions=(Exception,), return_on_failure=None, 
                           raise_on_failure=True):
    """
    Smarter retry that handles specific HTTP errors differently
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        logger.info(f"{func.__name__} succeeded after {attempt + 1} attempts")
                    return result
                except requests.exceptions.HTTPError as e:
                    last_exception = e
                    status_code = e.response.status_code if e.response else None
                    
                    # Don't retry on certain errors
                    if status_code == 404:
                        logger.debug(f"{func.__name__} got 404 - not retrying")
                        if raise_on_failure:
                            raise
                        return return_on_failure
                    elif status_code == 422:
                        logger.debug(f"{func.__name__} got 422 (likely duplicate) - not retrying")
                        if raise_on_failure:
                            raise
                        return return_on_failure
                    elif status_code == 504 and attempt > 0:
                        logger.warning(f"{func.__name__} got 504 after retry - assuming success")
                        return {"status": "assumed_success"}  # Assume it worked
                    
                    # Retry for other errors
                    if attempt < max_attempts - 1:
                        logger.warning(f"{func.__name__} failed with {status_code} (attempt {attempt + 1}/{max_attempts})")
                        time.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        if raise_on_failure:
                            raise
                        return return_on_failure
                        
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"{func.__name__} failed (attempt {attempt + 1}/{max_attempts}): {str(e)}")
                        time.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        if raise_on_failure:
                            raise
                        return return_on_failure
            
            if last_exception and raise_on_failure:
                raise last_exception
            return return_on_failure
                
        return wrapper
    return decorator




@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_materials(name: str) -> Optional[Dict]:
    """
    Fetches materials from the API with specified name parameter.
    
    Args:
        name (str): The name parameter to filter materials.
        
    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    params = {"name": name}
    url = f"{base_url}/api/materials"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_bins() -> Optional[Dict]:
    """
    Fetches all bins from the ItemPath API.

    Returns:
        dict: The JSON response containing bins data, or None if an error occurs.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    url = f"{base_url}/api/bins"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()
    return response.json()


@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_material_properties(name: str = "default") -> Optional[Dict]:
    """
    Fetches material properties from the API with specified name parameter.
    
    Args:
        name (str): Name of material properties to retrieve. Defaults to "default".
    
    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
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
    
    response = requests.get(url, headers=headers, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def update_material(material_id: str, newPropertyID: str) -> Optional[Dict]:
    """
    Updates material details in the ItemPath API.
    
    Args:
        material_id (str): ID of the material to update
        newPropertyID (str): New property ID to assign to the material
    
    Returns:
        dict: API response as dictionary, None if error occurs
    """
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

    response = requests.put(url, headers=headers, json=data, timeout=120)
    response.raise_for_status()
    return response.json()


@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_storage_rules(materialID: str) -> Optional[Dict]:
    """
    Fetches storage rules from the ItemPath API for a specific material.
    
    Args:
        materialID (str): The ID of the material to get storage rules for
    
    Returns:
        dict: The JSON response from the API as a Python dictionary containing storage rules,
              or None if an error occurs.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    url = f"{base_url}/api/storage_rules"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    params = {
        "limit": 10000,
        "materialId": materialID
    }

    response = requests.get(url, headers=headers, params=params, timeout=120)
    response.raise_for_status()
    
    return response.json()


@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
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
        is_default_bin (bool): Whether this is the default bin
        description (str): Description of the storage rule
    
    Returns:
        dict: API response as dictionary or None if error occurs
    """
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
    
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


# Additional helper functions that might be useful

@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def delete_storage_rule(storage_rule_id: str) -> Optional[Dict]:
    """
    Deletes a storage rule via the ItemPath API.
    
    Args:
        storage_rule_id (str): UUID of the storage rule to delete
    
    Returns:
        dict: API response as dictionary or None if error occurs
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in .env file")

    url = f"{base_url}/api/storage_rules/{storage_rule_id}"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.delete(url, headers=headers, timeout=120)
    response.raise_for_status()
    return response.json()


@smart_retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def update_storage_rule(
    storage_rule_id: str,
    updates: Dict[str, any]
) -> Optional[Dict]:
    """
    Updates a storage rule via the ItemPath API.
    
    Args:
        storage_rule_id (str): UUID of the storage rule to update
        updates (Dict[str, any]): Dictionary containing fields to update
    
    Returns:
        dict: API response as dictionary or None if error occurs
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in .env file")

    url = f"{base_url}/api/storage_rules/{storage_rule_id}"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.put(url, headers=headers, json=updates, timeout=120)
    response.raise_for_status()
    return response.json()


# Batch processing helper with retry logic
def process_materials_batch(material_names: List[str], property_id: str) -> Dict[str, bool]:
    """
    Process multiple materials in batch with retry logic.
    
    Args:
        material_names: List of material names to process
        property_id: Property ID to update materials with
    
    Returns:
        Dictionary mapping material names to success status
    """
    results = {}
    
    for material_name in material_names:
        try:
            # Get material details
            material_response = get_materials(material_name)
            
            if material_response and 'materials' in material_response and material_response['materials']:
                material_id = material_response['materials'][0]['id']
                
                # Update material property
                update_response = update_material(material_id, property_id)
                
                if update_response:
                    results[material_name] = True
                    logger.info(f"Successfully updated material: {material_name}")
                else:
                    results[material_name] = False
                    logger.error(f"Failed to update material: {material_name}")
            else:
                results[material_name] = False
                logger.error(f"Material not found: {material_name}")
                
        except Exception as e:
            results[material_name] = False
            logger.error(f"Error processing material {material_name}: {str(e)}")
    
    return results


# Helper function to validate API configuration
def validate_api_config() -> bool:
    """
    Validates that the API configuration is properly set.
    
    Returns:
        bool: True if configuration is valid, False otherwise
    """
    if not base_url or not auth_token:
        logger.error("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the environment")
        return False
    
    if not base_url.startswith(('http://', 'https://')):
        logger.error("ITEMPATH_URL must start with http:// or https://")
        return False
    
    if len(auth_token) < 10:  # Basic sanity check
        logger.error("ITEMPATH_TOKEN appears to be invalid")
        return False
    
    return True