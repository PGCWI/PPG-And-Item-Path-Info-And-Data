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

env_config = load_environment()

base_url = env_config['ITEMPATH_URL']
auth_token = env_config['ITEMPATH_APPLICATION_TOKEN']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry_on_exception(max_attempts: int = 5, initial_delay: float = 1.0, max_delay: float = 30.0, 
                      backoff_factor: float = 2.0, exceptions=(Exception,), 
                      return_on_failure=None, raise_on_failure=True):
    """
    Decorator to retry a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        backoff_factor: Factor to multiply delay by after each retry
        exceptions: Tuple of exceptions to catch and retry on
        return_on_failure: Value to return if all attempts fail (default: None)
        raise_on_failure: Whether to raise exception on failure (default: True)
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
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"{func.__name__} failed (attempt {attempt + 1}/{max_attempts}): {str(e)}")
                        time.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {str(e)}")
                        if raise_on_failure:
                            raise
                        else:
                            return return_on_failure
            
            # This should never be reached, but just in case
            if last_exception and raise_on_failure:
                raise last_exception
            return return_on_failure
                
        return wrapper
    return decorator



def get_latest_run_directory():
    """Helper function to get the latest run directory"""
    base_path = Path("data/runs")
    try:
        return max(base_path.rglob("metadata.json")).parent
    except ValueError:
        raise FileNotFoundError("No run directories found with metadata.json")


def load_batch_df():
    """Load the latest batch data from parquet file"""
    latest_run = get_latest_run_directory()
    return pd.read_parquet(latest_run / "abs_batches.parquet")


def save_batch_df(df, filename):
    """Save batch DataFrame to parquet in the latest run directory"""
    latest_run = get_latest_run_directory()
    file_path = latest_run / filename
    df.to_parquet(file_path, index=False)
    return file_path


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_ordersWAlloc(soNumber, limit: int = 1):
    """
    Fetches a list of orders from the API with specified parameters.
    
    Args:
        soNumber: The sales order number to search for
        limit: Maximum number of orders to fetch (default: 1)
        include_details: If True, returns tuple with allocation info (default: False for backward compatibility)
    
    Returns:
        If include_details is False: Returns order ID string or None (backward compatible)
        If include_details is True: Returns tuple (order_id, is_allocated, order_data)
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    params = {
        "limit": limit,
        "name": soNumber
    }
    url = f"{base_url}/api/orders"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=200)
    response.raise_for_status()
    orders = response.json().get('orders', [])
    
    if len(orders) == 0:
        return None, None
    
    order = orders[0]
    order_id = order['id']
    
    # Check allocation status from the order data
    allocationStatus = order.get('status', '')
    
    if allocationStatus in ['Ready for Allocation', 'Untouched']:
        is_allocated = False
    elif allocationStatus in ['Is Allocated', 'In Process']:
        is_allocated = True
    else:
        is_allocated = None
    
    return order_id, is_allocated

@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_orders(soNumber, limit: int = 1):
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    params = {
        "limit": limit,
        "name": soNumber
    }

    url = f"{base_url}/api/orders"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, params=params, timeout=200)
    response.raise_for_status()

    orders = response.json().get('orders', [])
    if len(orders) == 0:
        return None
    else:
        return orders[0]['id']

@retry_on_exception(max_attempts=2, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_order_by_id(order_id):
    """
    Fetches the order details for a given order ID from the API.
    """
    if not base_url or not auth_token:
        raise ValueError("API_URL and AUTH_TOKEN must be set in the .env file")

    url = f"{base_url}/api/orders/{order_id}"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, timeout=300)
    response.raise_for_status()
    return response.json()


def check_if_orderIsAllocated(order_id):
    """
    Check if an order is allocated. This function uses get_order_by_id which already has retry logic.
    """
    try:
        orderDetail = get_order_by_id(order_id)
        
        if orderDetail is None:
            return None, orderDetail
            
        allocationStatus = orderDetail['order']['status']

        if allocationStatus == 'Ready for Allocation' or allocationStatus == 'Untouched':
            return False, orderDetail    
        elif allocationStatus == 'Is Allocated' or allocationStatus == 'In Process':
            return True, orderDetail
        else:
            return None, orderDetail
            
    except Exception as e:
        logger.error(f"Error checking allocation status for order {order_id}: {str(e)}")
        return None, None


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def allocate_order(order_id, allocateOrDeallocate=True):
    """
    Allocates or deallocates the order with the given ID.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    url = f"{base_url}/api/orders/{order_id}"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    data = {
        "allocate": True if allocateOrDeallocate else False,
        "deallocate": True if not allocateOrDeallocate else False
    }
    
    response = requests.put(url, headers=headers, json=data, timeout=200)
    response.raise_for_status()
    return response.json()


def create_and_validate_batch(name: str):
    """
    Creates and validates a batch with improved retry logic and return consistency.
    
    Args:
        name: Name of the batch to create
        
    Returns:
        str: The batch ID if successful, None if failed
    """
    # First check if batch already exists
    batch_id = get_batches(name)
    if batch_id:
        return batch_id
    
    # Try to create the batch with retries
    max_validation_attempts = 20  # Reduced from 60 for faster failure
    
    for attempt in range(3):  # Try creating the batch up to 3 times
        try:
            create_result = create_batch(name)
            if create_result:
                logger.info(f"Batch '{name}' created successfully")
                
                # Wait a bit for the batch to be available
                time.sleep(2)
                
                # Validate the batch was created
                for validation_attempt in range(max_validation_attempts):
                    batch_id = get_batches(name)
                    if batch_id:
                        return batch_id
                    time.sleep(1)
                    
        except Exception as e:
            logger.error(f"Error creating batch '{name}' (attempt {attempt + 1}): {str(e)}")
            
    logger.error(f"Failed to create and validate batch '{name}' after all attempts")
    return None


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def create_batch(name: str):
    """
    Creates a new batch with the given name.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    url = f"{base_url}/api/batches"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    data = {
        "name": name,
        "type": 2
    }
    
    response = requests.post(url, headers=headers, json=data, timeout=200)
    response.raise_for_status()
    return response.json()


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_batches(batch_name: str, limit: int = 1):
    """
    Fetches a list of batches from the API filtered by batch name.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    url = f"{base_url.rstrip('/')}/api/batches"

    params = {
        "limit": limit,
        "name": batch_name
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, params=params, timeout=200)
    response.raise_for_status()
    
    batches = response.json().get('batches', [])
    if batches:
        return batches[0]['id']
    else:
        logger.debug(f"No batches found with name: {batch_name}")
        return None


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_work_order_lines(id):
    """
    Fetches a list of work order lines from the API filtered by order ID.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    url = f"{base_url.rstrip('/')}/api/work_order_lines"

    params = {
        "orderId": id
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, params=params, timeout=200)
    response.raise_for_status()
    
    work_order_lines = response.json().get('work_order_lines', [])
    return work_order_lines if work_order_lines else None


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def update_work_order_line(work_order_line_id, batch_id, handling_rank=1):
    """
    Updates a work order line with batch and handling rank information.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    url = f"{base_url.rstrip('/')}/api/work_order_lines/{work_order_line_id}"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    data = {
        "batchId": batch_id,
        "handlingRank": handling_rank
    }

    response = requests.put(url, headers=headers, json=data, timeout=200)
    response.raise_for_status()
    return response.json()


def update_work_order_line_handler(work_order_line_id, batch_id, handling_rank=1, 
                                 batch_name="", max_attempts=10, delay_seconds=1):
    """
    Fixed handler with more efficient retry logic
    """
    # If batch_id is None, try to get it
    if not batch_id and batch_name:
        batch_id = get_batches(batch_name)
        if not batch_id:
            logger.error(f"Batch {batch_name} not found for work order line update")
            return None
    
    if not batch_id:
        logger.error("No batch_id provided for work order line update")
        return None
    
    # The update_work_order_line already has retry logic, so we just need to handle
    # the case where the batch might not be ready yet
    for attempt in range(max_attempts):
        try:
            result = update_work_order_line(work_order_line_id, batch_id, handling_rank)
            if result:
                if attempt > 0:
                    logger.info(f"Work order line {work_order_line_id} updated after {attempt + 1} attempts")
                return result
        except Exception as e:
            if "batch" in str(e).lower() and attempt < max_attempts - 1:
                # Batch might not be ready, wait and retry
                time.sleep(delay_seconds)
                # Try to refresh batch_id
                if batch_name:
                    new_batch_id = get_batches(batch_name)
                    if new_batch_id:
                        batch_id = new_batch_id
            else:
                # Other error or final attempt
                if attempt == max_attempts - 1:
                    logger.error(f"Failed to update work order line {work_order_line_id}: {e}")
                    return None
    
    return None


def update_work_order_lines_bulk(work_order_lines, batch_id, batch_name=""):
    """
    Update multiple work order lines more efficiently
    """
    if not work_order_lines:
        return True
    
    # Ensure we have a valid batch_id
    if not batch_id and batch_name:
        batch_id = get_batches(batch_name)
    
    if not batch_id:
        logger.error("No valid batch_id for bulk update")
        return False
    
    success_count = 0
    
    # Use ThreadPoolExecutor for parallel updates
    with ThreadPoolExecutor(max_workers=min(5, len(work_order_lines))) as executor:
        future_to_line = {
            executor.submit(
                update_work_order_line_handler,
                line['id'],
                batch_id,
                idx + 1,  # handling rank
                batch_name
            ): line
            for idx, line in enumerate(work_order_lines)
        }
        
        for future in as_completed(future_to_line):
            line = future_to_line[future]
            try:
                result = future.result()
                if result:
                    success_count += 1
                else:
                    logger.warning(f"Failed to update work order line {line['id']}")
            except Exception as e:
                logger.error(f"Exception updating work order line {line['id']}: {e}")
    
    success_rate = success_count / len(work_order_lines)
    if success_rate < 1.0:
        logger.warning(f"Only {success_count}/{len(work_order_lines)} work order lines updated successfully")
    
    return success_rate > 0.8  # Consider it successful if 80% or more succeeded


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def get_materials(name: str) -> Optional[Dict]:
    """
    Fetches materials from the API with specified name parameter.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    params = {"name": name}
    url = f"{base_url}/api/materials"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=200)
    response.raise_for_status()
    return response.json()


@retry_on_exception(max_attempts=5, initial_delay=1.0, exceptions=(requests.RequestException, Exception), raise_on_failure=False, return_on_failure=None)
def update_order_line_storageUnits(order_line_id, storageUnitArrayString):
    """
    Updates the storage units for an order line.
    """
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    url = f"{base_url.rstrip('/')}/api/order_lines/{order_line_id}"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    data = {
        "storageUnits": storageUnitArrayString
    }
    
    response = requests.put(url, headers=headers, json=data, timeout=200)
    response.raise_for_status()
    return response.json()


def process_storage_units(order_id):
    """
    Process and update storage units for an order.
    This function uses other functions that already have retry logic.
    """
    try:
        order = get_order_by_id(order_id)
        if not order:
            logger.error(f"Order {order_id} not found")
            return None
            
        order_lines = pd.DataFrame(order['order']['order_lines'])

        jersey_picking_units = [{"name": f"G{i}"} for i in range(1, 5)]
        ln_picking_units = [{"name": f"V{i}"} for i in range(1, 6)]
        
        updated_count = 0
        for index, row in order_lines.iterrows():
            material_name = row['materialName']
            
            try:
                material_detail = get_materials(material_name)
                
                if material_detail and 'materials' in material_detail:
                    group_code = material_detail['materials'][0]['GroupCode']
                    
                    storage_units = None
                    if group_code == '121':
                        storage_units = jersey_picking_units
                    elif group_code == '112':
                        storage_units = ln_picking_units
                        
                    if storage_units:
                        logger.info(f"Updating storage units for order line {row['id']}")
                        update_order_line_storageUnits(row['id'], storage_units)
                        updated_count += 1
                        
            except Exception as e:
                logger.error(f"Error processing material {material_name}: {str(e)}")
                # Continue with next material instead of failing completely
                continue

        logger.info(f"Updated storage units for {updated_count} order lines")
        return order_lines
        
    except Exception as e:
        logger.error(f"Error processing storage units for order {order_id}: {str(e)}")
        return None


def returnOrderLines(order_id=None, orderJson=None, returnJSONORDF=False):
    """
    Return order lines either as JSON or DataFrame.
    This function uses get_order_by_id which already has retry logic.
    """
    try:
        if order_id is None and orderJson is None:
            return None
            
        if orderJson is None:
            orderJson = get_order_by_id(order_id)
            
        if not orderJson or 'order' not in orderJson or 'order_lines' not in orderJson['order']:
            logger.error("Invalid order JSON structure")
            return None

        if returnJSONORDF:
            return orderJson['order']['order_lines']
        else:
            return pd.DataFrame(orderJson['order']['order_lines'])
            
    except Exception as e:
        logger.error(f"Error returning order lines: {str(e)}")
        return None