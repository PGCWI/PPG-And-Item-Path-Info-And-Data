import requests
import json
import time
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta
import logging

def create_count_order_by_location(
    api_base_url: str,
    access_token: str,
    order_name: str,
    location_ids: List[str],
    priority: int = 2,
    deadline: Optional[datetime] = None,
    info_fields: Optional[Dict[str, str]] = None,
    debug: bool = False
) -> Dict:
    """
    Create a count order for materials at specific locations using location IDs.
    
    Args:
        api_base_url: Base URL for the API (e.g., "https://subdomain.itempath.com/api")
        access_token: JWT Access Token for authentication
        order_name: Unique order name/number
        material_ids: List of material IDs to count
        location_ids: List of location IDs to count in
        priority: Order priority (1=Low, 2=Medium, 3=High, 4=Hot)
        deadline: Optional deadline for order completion (defaults to today + 1 business day)
        info_fields: Optional dictionary containing Info1-Info5 values
        debug: If True, prints the payload and response for debugging
        
    Returns:
        Response from the API as a dictionary, or a dictionary with an 'already_exists' flag if the order already exists
        
    Raises:
        requests.exceptions.HTTPError: If the API returns an error status code after all retries
        ValueError: If material_ids or location_ids lists are empty
    """
    
    if not location_ids:
        raise ValueError("At least one location ID must be provided")
    
    # Remove trailing slash from base URL if present
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
        
    # Format locations
    formatted_locations = [{"id": loc_id} for loc_id in location_ids]
    
    # Construct the order payload
    payload = {
        "name": order_name,
        "directionType": 5,  # Count order as integer
        "priority": priority,
        "locations": formatted_locations
    }
    
    # Set deadline to today + 1 business day if not provided
    if deadline is None:
        today = datetime.now()
        # Calculate next business day (skip weekends)
        days_to_add = 1
        if today.weekday() == 4:  # Friday
            days_to_add = 3  # Skip to Monday
        elif today.weekday() == 5:  # Saturday
            days_to_add = 2  # Skip to Monday
        
        deadline = today + timedelta(days=days_to_add)
    
    # Add deadline (required)
    payload["deadline"] = deadline.isoformat()
    
    # Add info fields if provided
    if info_fields:
        for i, (key, value) in enumerate(info_fields.items(), 1):
            if i <= 5:  # Only Info1 through Info5 are supported
                payload[f"Info{i}"] = value
    
    # Set up headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    # Add retry logic (up to 5 attempts with 2 second wait between retries)
    max_retries = 5
    retry_count = 0
    last_exception = None
    
    while retry_count < max_retries:
        try:
            # Make the API request
            if debug:
                print(f"Request URL: {api_base_url}/orders")
                print(f"Request Headers: {headers}")
                print(f"Request Payload: {json.dumps(payload, indent=2, default=str)}")
                if retry_count > 0:
                    print(f"Retry attempt {retry_count} of {max_retries}")
            
            response = requests.post(
                f"{api_base_url}/orders",
                headers=headers,
                json=payload,
                timeout=(120, 2500)  # (connect timeout, read timeout) in seconds
            )
            
            if debug:
                print(f"Response Status: {response.status_code}")
                print(f"Response Content: {response.text}")
            
            # Check if response status is 200
            if response.status_code == 200:
                # Success - return the response JSON
                return response.json()
            # Check if order already exists (specific 422 error)
            elif response.status_code == 422:
                try:
                    error_details = response.json()
                    # Check if the error is due to duplicate order name
                    if "errors" in error_details and isinstance(error_details["errors"], list):
                        for error in error_details["errors"]:
                            if isinstance(error, dict) and "name" in error:
                                name_error = error["name"]
                                if (isinstance(name_error, dict) and 
                                    "message" in name_error and 
                                    "already exists" in name_error["message"]):
                                    if debug:
                                        print(f"Order '{order_name}' already exists. Skipping.")
                                    # Return a dictionary indicating the order already exists
                                    return {"already_exists": True, "order_name": order_name}
                except Exception as e:
                    if debug:
                        print(f"Error parsing 422 response: {str(e)}")
                
                # If we get here, it's a 422 error but not due to duplicate order name
                # Generate error message
                error_message = f"{response.status_code} {response.reason} for url: {response.url}"
                try:
                    error_message += f"\nDetails: {json.dumps(error_details, indent=2)}"
                except Exception:
                    error_message += f"\nResponse Text: {response.text}"
            # Check for "must have at least one order line" error (400 error)
            elif response.status_code == 400:
                try:
                    error_details = response.json()
                    print(error_details)
                    # Check if the error mentions order lines
                    if "Order must have at least one order line." in error_details:
                        if debug:
                            print(f"Order '{order_name}' cannot be created: No order lines available. Skipping.")
                        # Return a dictionary indicating the order has no lines
                        return {"no_order_lines": True, "order_name": order_name}
                except Exception as e:
                    if debug:
                        print(f"Error parsing 400 response: {str(e)}")
                
                # If we get here, it's a 400 error but not due to missing order lines
                # Generate error message
                error_message = f"{response.status_code} {response.reason} for url: {response.url}"
                try: 
                    error_message += f"\nDetails: {json.dumps(error_details, indent=2)}"
                except Exception:
                    error_message += f"\nResponse Text: {response.text}"
                
                # If we haven't exceeded max retries, sleep and retry
                if retry_count < max_retries - 1:
                    if debug:
                        print(f"Error: {error_message}")
                        print(f"Waiting 2 seconds before retry {retry_count + 1}...")
                    
                    time.sleep(2)  # Wait 2 seconds before retrying
                    retry_count += 1
                else:
                    # We've exhausted our retries, raise the error
                    #raise requests.exceptions.HTTPError(error_message, response=response)
                    return {"no_order_lines": False, "order_name": order_name, "failed":False}
            else:
                # Other response error - generate error message
                error_message = f"{response.status_code} {response.reason} for url: {response.url}"
                try:
                    error_details = response.json()
                    error_message += f"\nDetails: {json.dumps(error_details, indent=2)}"
                except Exception:
                    error_message += f"\nResponse Text: {response.text}"
                
                # If we haven't exceeded max retries, sleep and retry
                if retry_count < max_retries - 1:
                    if debug:
                        print(f"Error: {error_message}")
                        print(f"Waiting 2 seconds before retry {retry_count + 1}...")
                    
                    time.sleep(2)  # Wait 2 seconds before retrying
                    retry_count += 1
                else:
                    # We've exhausted our retries, raise the error
                    #raise requests.exceptions.HTTPError(error_message, response=response)
                    return {"no_order_lines": False, "order_name": order_name, "failed":True}
                    
        except requests.exceptions.RequestException as e:
            last_exception = e
            
            if debug:
                print(f"Error: {str(e)}")
                print(f"Waiting 60 seconds before retry {retry_count + 1}...")
            
            # If we haven't exceeded max retries, sleep and retry
            if retry_count < max_retries - 1:
                time.sleep(60)  # Wait 2 seconds before retrying
                retry_count += 1
            else:
                # We've exhausted our retries, re-raise the exception
                #raise
                return {"no_order_lines": False, "order_name": order_name, "failed":True}
    
    # This should not be reached, but just in case
    if last_exception:
        raise last_exception
    else:
        #raise Exception("Unknown error occurred during API request after all retries")
        return {"no_order_lines": False, "order_name": order_name, "failed":True}

def delete_order(
    api_base_url: str,
    access_token: str,
    order_id: str,
    debug: bool = False
) -> Dict:
    """
    Delete a specific order from the ItemPath database.
    This will also delete all order lines associated with the order.
    
    Args:
        api_base_url: Base URL for the API (e.g., "https://subdomain.itempath.com/api")
        access_token: JWT Access Token for authentication
        order_id: The ID of the order to delete
        debug: If True, prints the request and response details for debugging
        
    Returns:
        Response from the API as a dictionary
        
    Raises:
        requests.exceptions.HTTPError: If the API returns an error status code
    """
    # Remove trailing slash from base URL if present
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    
    # Set up headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    # Construct the URL
    url = f"{api_base_url}/orders/{order_id}/delete"
    
    # Make the API request
    if debug:
        print(f"Request URL: {url}")
        print(f"Request Headers: {headers}")
    
    try:
        response = requests.delete(
            url,
            headers=headers
        )
        
        if debug:
            print(f"Response Status: {response.status_code}")
            print(f"Response Content: {response.text}")
        
        # Check for errors
        if response.status_code >= 400:
            error_message = f"{response.status_code} {response.reason} for url: {response.url}"
            try:
                error_details = response.json()
                error_message += f"\nDetails: {error_details}"
            except Exception:
                error_message += f"\nResponse Text: {response.text}"
                
            raise requests.exceptions.HTTPError(error_message, response=response)
        
        # Return the response JSON
        return response.json()
        
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"Error: {str(e)}")
        raise

def get_order_id_by_name(
    api_base_url: str,
    access_token: str,
    order_name: str,
    limit: int = 1,
    debug: bool = False
) -> Optional[str]:
    """
    Get an order ID based on the order name/number.
    
    Args:
        api_base_url: Base URL for the API (e.g., "https://subdomain.itempath.com/api")
        access_token: JWT Access Token for authentication
        order_name: The order name/number to search for
        limit: The maximum number of results to return
        debug: If True, prints the request and response details for debugging
        
    Returns:
        The order ID as a string if found, None if not found
        
    Raises:
        requests.exceptions.HTTPError: If the API returns an error status code
        ValueError: If required parameters are missing
    """
    # Validate inputs
    if not api_base_url or not access_token or not order_name:
        raise ValueError("api_base_url, access_token, and order_name are required")
    
    # Remove trailing slash from base URL if present
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    
    # Construct the query parameters
    params = {
        "limit": limit,
        "name": order_name
    }
    
    # Construct the URL
    url = f"{api_base_url}/orders"
    
    # Set up headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    # Make the API request
    if debug:
        print(f"Request URL: {url}")
        print(f"Request Parameters: {params}")
        print(f"Request Headers: {headers}")
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params
        )
        
        if debug:
            print(f"Response Status: {response.status_code}")
            print(f"Response Content: {response.text[:200]}...")  # First 200 chars
        
        # Check for errors
        if response.status_code >= 400:
            error_message = f"{response.status_code} {response.reason} for url: {response.url}"
            try:
                error_details = response.json()
                error_message += f"\nDetails: {error_details}"
            except Exception:
                error_message += f"\nResponse Text: {response.text}"
                
            raise requests.exceptions.HTTPError(error_message, response=response)
        
        # Parse the response
        response_data = response.json()
        
        # Check if any orders were returned
        if not response_data.get('orders') or len(response_data['orders']) == 0:
            if debug:
                print(f"No orders found with name: {order_name}")
            return None
        
        # Return the ID of the first matching order
        order_id = response_data['orders'][0]['id']
        
        if debug:
            print(f"Found order ID: {order_id}")
            
        return order_id
        
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"Error fetching orders: {str(e)}")
        raise

def get_transactions(
    api_base_url: str,
    access_token: str,
    order_name: Optional[str] = None,
    limit: int = 1000,
    page: int = 0,
    transaction_type: Optional[str] = None,
    storageUnitName: Optional[str] = None,
    creation_date_from: Optional[str] = None,
    creation_date_to: Optional[str] = None,
    debug: bool = False,
    countOnly: bool = False,
    **kwargs
) -> Dict:
    """
    Get transactions from the ItemPath API filtered by various parameters.
    
    Args:
        api_base_url: Base URL for the API (e.g., "https://subdomain.itempath.com/api")
        access_token: JWT Access Token for authentication
        order_name: Optional name of the order to filter transactions by
        limit: Maximum number of transactions to return (default: 1000)
        page: Page number for pagination (default: 0)
        export_state_type: Optional filter by export state type:
            0 = NotSet
            1 = Cannot yet be exported
            2 = Ready to export
            3 = Currently exporting or export canceled
            4 = Successfully exported
            5 = Export failed
            6 = Not to be exported
        transaction_type: Optional filter by transaction type:
            0 = NotSet
            1 = ManualPut
            2 = ManualPick
            3 = OrderPut
            4 = OrderPick
            5 = Transfer
            6 = OrderCount
            7 = ContextCount
            8 = MaterialRename
            9 = ManualCorrection
            10 = ContextCorrection
            11 = CancelRequest
            12 = Purge
            13 = Production
            15 = KitRename
        creation_date_from: Filter transactions created after this date (format: YYYY-MM-DD)
                          Use this to get transactions after a specific date
        creation_date_to: Filter transactions created before this date (format: YYYY-MM-DD)
                        Use this to get transactions before a specific date
        debug: If True, prints the request and response details for debugging
        **kwargs: Additional filter parameters as described in the API documentation
        
    Returns:
        Response from the API as a dictionary containing the transactions list
        
    Raises:
        requests.exceptions.HTTPError: If the API returns an error status code
        ValueError: If required parameters are missing
    """
    # Validate inputs
    if not api_base_url or not access_token:
        raise ValueError("api_base_url and access_token are required")
    
    # Remove trailing slash from base URL if present
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    
    # Construct the query parameters
    params = {
        "limit": limit,
        "page": page,
    }
    
    # Add optional filters if provided
    if order_name:
        params["orderName"] = order_name
    
    if transaction_type is not None:
        params["type"] = transaction_type
    
    if storageUnitName is not None:
        params["storageUnitName"] = storageUnitName

    # Add date filters with operators
    if creation_date_from:
        params["creationDate"] ="[gt]" + creation_date_from
    
    if creation_date_to:
        params["creationDate"] = "[lt]" +creation_date_to

    params["motiveType"] = "[not]5"
    
    # Add any additional filters from kwargs
    params.update(kwargs)
    
    # Construct the URL
    url = f"{api_base_url}/transactions"
    
    # Set up headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    # Make the API request
    if debug:
        print(f"Request URL: {url}")
        print(f"Request Parameters: {params}")
        print(f"Request Headers: {headers}")
    
    try:
        print(url)
        print(headers)
        print(params)


        response = requests.get(
            url,
            headers=headers,
            params=params
        )
        
        if debug:
            print(f"Response Status: {response.status_code}")
            print(f"Response Content: {response.text[:200]}...")  # First 200 chars
        
        # Check for errors
        if response.status_code >= 400:
            error_message = f"{response.status_code} {response.reason} for url: {response.url}"
            try:
                error_details = response.json()
                error_message += f"\nDetails: {json.dumps(error_details, indent=2)}"
            except Exception:
                error_message += f"\nResponse Text: {response.text}"
                
            raise requests.exceptions.HTTPError(error_message, response=response)
        
        # Parse the response
        response_data = response.json()
        
        if debug:
            print(f"Found {len(response_data.get('transactions', []))} transactions")
            
        return response_data
        
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"Error fetching transactions: {str(e)}")
        raise

def get_transactions_df(
    api_base_url: str,
    access_token: str,
    order_name: Optional[str] = None,
    transaction_type: Optional[str] = None,
    storageUnitName: Optional[str] = None,
    page_size: int = 1000,
    creation_date_from: Optional[str] = None,
    creation_date_to: Optional[str] = None,
    debug: bool = False,
    **kwargs):
    """
    Get ALL transactions from the ItemPath API and return as a pandas DataFrame,
    handling pagination automatically.
    
    Args:
        api_base_url: Base URL for the API (e.g., "https://subdomain.itempath.com/api")
        access_token: JWT Access Token for authentication
        order_name: Optional name of the order to filter transactions by
        transaction_type: Optional filter by transaction type
        page_size: Number of records per page (default: 1000)
        creation_date_from: Filter transactions created after this date (format: YYYY-MM-DD)
        creation_date_to: Filter transactions created before this date (format: YYYY-MM-DD)
        debug: If True, prints the request and response details for debugging
        **kwargs: Additional filter parameters as described in the API documentation
        
    Returns:
        pandas DataFrame containing ALL transaction data across all pages
        
    Raises:
        requests.exceptions.HTTPError: If the API returns an error status code
        ImportError: If pandas is not installed
        ValueError: If required parameters are missing
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("This function requires pandas. Install it with 'pip install pandas'")
    
    # Initialize an empty list to store all dataframes
    all_dfs = []
    page = 0
    more_records = True
    total_records = 0
    
    logging.info(f"Starting transaction data fetch with page size: {page_size}")
    
    while more_records:
        if debug:
            print(f"Fetching page {page}...")
        
        # Get transactions for current page
        response_data = get_transactions(
            api_base_url=api_base_url,
            access_token=access_token,
            order_name=order_name,
            transaction_type=transaction_type,
            storageUnitName=storageUnitName,
            creation_date_from=creation_date_from,
            creation_date_to=creation_date_to,
            limit=page_size,
            page=page,
            debug=debug,
            **kwargs
        )
        
        # Extract transactions list
        transactions = response_data.get('transactions', [])
        
        # Check if we have any transactions
        if not transactions:
            logging.info(f"No more transactions found on page {page}")
            if debug:
                print(f"No more transactions found on page {page}")
            more_records = False
        else:
            # Convert to DataFrame and append to list
            df = pd.DataFrame(transactions)
            all_dfs.append(df)
            
            # Update our total count
            total_records += len(transactions)
            print(total_records)
            logging.info(f"Fetched {len(transactions)} records on page {page}. Total so far: {total_records}")
            
            if debug:
                print(f"Fetched {len(transactions)} records on page {page}. Total so far: {total_records}")
            
            # Check if we got less than page_size records (indicates last page)
            if len(transactions) < page_size:
                if debug:
                    print(f"Last page reached with {len(transactions)} records")
                logging.info(f"Last page reached with {len(transactions)} records. Stopping pagination.")
                more_records = False
            else:
                # Increment page for next iteration
                page += 1
    
    # Combine all dataframes
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        logging.info(f"Total records fetched: {len(final_df)}")
        if debug:
            print(f"Total records fetched: {len(final_df)}")
        return final_df
    else:
        logging.info("No transactions found")
        if debug:
            print("No transactions found")
        return pd.DataFrame()

"""
def get_transactions_df(
    api_base_url: str,
    access_token: str,
    order_name: Optional[str] = None,
    transaction_type: Optional[int] = None,
    limit: int = 1000,
    minDate=None,
    debug: bool = False,
    **kwargs):
    
    Get transactions from the ItemPath API and return as a pandas DataFrame.
    
    Args:
        api_base_url: Base URL for the API (e.g., "https://subdomain.itempath.com/api")
        access_token: JWT Access Token for authentication
        order_name: Optional name of the order to filter transactions by
        limit: Maximum number of transactions to return (default: 1000)
        debug: If True, prints the request and response details for debugging
        **kwargs: Additional filter parameters as described in the API documentation
        
    Returns:
        pandas DataFrame containing the transaction data
        
    Raises:
        requests.exceptions.HTTPError: If the API returns an error status code
        ImportError: If pandas is not installed
        ValueError: If required parameters are missing
    
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("This function requires pandas. Install it with 'pip install pandas'")
    
    # Get the transactions data
    response_data = get_transactions(
        api_base_url=api_base_url,
        access_token=access_token,
        order_name=order_name,
        transaction_type=transaction_type,
        limit=limit,
        creation_date_from=minDate, 
        debug=debug,
        **kwargs
    )
    
    # Extract transactions list
    transactions = response_data.get('transactions', [])
    
    if not transactions:
        if debug:
            print("No transactions found")
        # Return empty DataFrame with expected columns
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions)
    
    return df
"""