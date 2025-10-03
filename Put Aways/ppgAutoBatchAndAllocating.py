import pandas as pd
import requests
from typing import Dict, Optional
import os 
from dotenv import load_dotenv
import time

def get_orders(soNumber, limit: int = 1):
    """
    Fetches a list of orders from the API with specified parameters.

    Args:
        limit (int): The number of orders to retrieve.
        direction_type (int, optional): Filters orders by direction type.

    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")  # e.g., http://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")

    # Construct the full URL with query parameters
    params = {
        "limit": limit,
        "name": soNumber
    }
    #if direction_type is not None:
    #    params["directionType"] = direction_type

    url = f"{base_url}/api/orders"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        # Make the GET request with query parameters
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

        #check if orders are returned
        if len(response.json()['orders']) == 0:
            return None
        else:
            orderID= response.json()['orders'][0]['id']
            return orderID #response.json()  # Return the JSON response as a dictionary
    except requests.exceptions.RequestException as e:
        print(f"Error fetching orders: {e}")
        return None

def get_order_by_id(order_id):
    """
    Fetches the order details for a given order ID from the API.
    
    Args:
        order_id (str): The ID of the order to fetch.
        
    Returns:
        dict: The JSON response from the API as a Python dictionary.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")  # e.g., http://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for 

    if not base_url or not auth_token:
        raise ValueError("API_URL and AUTH_TOKEN must be set in the .env file")

    # Construct the full URL
    url = f"{base_url}/api/orders/{order_id}"
    
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
        print(f"Error fetching order: {e}")
        return None
    
def check_if_orderIsAllocated(order_id):
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
    return

def allocate_order(order_id, allocateOrDeallocate=True):
    """
    Allocates the order with the given ID by sending a PUT request to the API.
    
    Args:
        order_id (str): The ID of the order to allocate.
        handling_unit (str): The handling unit to use for allocation.
    
    Returns:
        Optional[dict]: The JSON response from the API, or None if an error occurs.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    # Construct the full URL
    url = f"{base_url}/api/orders/{order_id}"
    
    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    # Data to send in the request body
    if allocateOrDeallocate:
        data = {
            "allocate": True
        }
    else:
        data = {
            "allocate": False,
            "deallocate": True
        }
    
    try:
        # Make the PUT request
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error allocating order: {e}")
        return None
    

def create_and_validate_batch(name: str):
    batch_id = get_batches(name)
    i = 10

    while not batch_id and i > 0:
        x = create_batch(name)
        time.sleep(1)
        batch_id = get_batches(name)
        if batch_id is not None:
            return True
        i -= 1
    
    return False
###TESTING
def create_batch(name: str):
    """
    Creates a new batch with the given name by sending a POST request to the API.
    
    Args:
        name (str): The name of the batch to create.
    
    Returns:
        Optional[dict]: The JSON response from the API, or None if an error occurs.
    """
    # Load environment variables
    load_dotenv()
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    # Construct the full URL
    url = f"{base_url}/api/batches"
    
    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    # Data to send in the request body
    data = {
        "name": name,
        "type": 2
    }
    
    try:
        # Make the POST request
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None #response.text
    except requests.exceptions.RequestException as e:
        print(f"Error creating batch: {e}")
        return None

def update_order_line_lot(order_line_id, batch):
    """
    Updates the 'lot' field of an existing order line by sending a PUT request to the API.
    
    Args:
        order_line_id (str): The ID of the order line to update.
        lot (str): The lot value to set for the order line.
    
    Returns:
        Optional[dict]: The JSON response from the API, or None if an error occurs.
    """
    # Load environment variables
    load_dotenv()
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    # Ensure base_url does not end with a trailing slash
    base_url = base_url.rstrip('/')
    
    # Construct the full URL
    url = f"{base_url}/api/order_lines/{order_line_id}"
    
    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    # Data to send in the request body
    data = {
        "batchId": batch
    }
    
    try:
        # Make the PUT request
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        print(response.text)
        return response.json()  # Return the JSON response as a dictionary
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error updating order line: {e}")
        return None
    
def update_order_line_storageUnits(order_line_id, storageUnitArrayString):
    """
    Updates the 'lot' field of an existing order line by sending a PUT request to the API.
    
    Args:
        order_line_id (str): The ID of the order line to update.
        lot (str): The lot value to set for the order line.
    
    Returns:
        Optional[dict]: The JSON response from the API, or None if an error occurs.
    """
    # Load environment variables
    load_dotenv()
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")
    
    # Ensure base_url does not end with a trailing slash
    base_url = base_url.rstrip('/')
    
    # Construct the full URL
    url = f"{base_url}/api/order_lines/{order_line_id}"
    
    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    
    # Data to send in the request body
    data = {
        "storageUnits": '[{"name": "G1"}]'
    }
    
    try:
        # Make the PUT request
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        print(response.text)
        return response.json()  # Return the JSON response as a dictionary
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error updating order line: {e}")
        return None

def get_batch_by_id(batch_id):
    """
    Retrieves the batch details for a given batch ID from the API.

    Args:
        batch_id (str): The ID of the batch to retrieve.

    Returns:
        Optional[dict]: The JSON response from the API as a Python dictionary,
                        or None if an error occurs.
    """
    # Load environment variables
    load_dotenv()
    base_url = os.getenv("ITEMPATH_URL")
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    # Ensure base_url does not end with a trailing slash
    base_url = base_url.rstrip('/')

    # Construct the full URL
    url = f"{base_url}/api/batches/{batch_id}"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        # Make the GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()  # Return the JSON response as a dictionary
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving batch: {e}")
        return None

def get_batches(batch_name: str, limit: int = 1):
    """
    Fetches a list of batches from the API filtered by batch name.

    Args:
        batch_name (str): The name of the batch to search for.
        limit (int): The number of batches to retrieve.

    Returns:
        Optional[str]: The ID of the first batch in the response,
                       or None if an error occurs.
    """
    # Load environment variables
    base_url = os.getenv("ITEMPATH_URL")  # e.g., https://subdomain.itempath.com
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    # Ensure base_url does not end with a trailing slash
    base_url = base_url.rstrip('/')

    # Construct the full URL with query parameters
    params = {
        "limit": limit,
        "name": batch_name
    }

    url = f"{base_url}/api/batches"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        # Make the GET request with query parameters
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        print(response.json())
        batches = response.json().get('batches', [])
        if batches:
            batch_id = batches[0]['id']
            return batch_id
        else:
            print("No batches found.")
            return None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching batches: {e}")
        return None

def get_work_order_lines(id):
    """
    Fetches a list of work order lines from the API filtered by batch ID.

    Args:
        batch_id (str): The ID of the batch to filter the work order lines.

    Returns:
        Optional[List[Dict]]: A list of work order lines as dictionaries,
                              or None if an error occurs.
    """
    # Load environment variables
    load_dotenv()
    base_url = os.getenv("ITEMPATH_URL")  # e.g., https://subdomain.itempath.com
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    # Ensure base_url does not end with a trailing slash
    base_url = base_url.rstrip('/')

    # Construct the full URL with query parameters

    params = {
        "orderId": id
    }

    url = f"{base_url}/api/work_order_lines"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        # Make the GET request with query parameters
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        
        work_order_lines = response.json().get('work_order_lines', [])
        if work_order_lines:
            return work_order_lines
        else:
            print("No work order lines found.")
            return None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching work order lines: {e}")
        return None

def update_work_order_line(work_order_line_id, batch_id, handling_rank=1):
    """
    Updates a work order line by sending a PUT request to the API.

    Args:
        work_order_line_id (str): The ID of the work order line to update.
        handling_unit (Optional[str]): The handling unit value to set (or None to clear it).
        handling_rank (Optional[str]): The handling rank value to set.
        batch_id (Optional[str]): The batch ID to set (or None to clear it).

    Returns:
        Optional[dict]: The JSON response from the API as a dictionary, or None if an error occurs.
    """
    # Load environment variables
    load_dotenv()
    base_url = os.getenv("ITEMPATH_URL")  # e.g., https://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication

    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_ACCESSTOKEN must be set in the .env file")

    # Ensure base_url does not end with a trailing slash
    base_url = base_url.rstrip('/')

    # Construct the full URL
    url = f"{base_url}/api/work_order_lines/{work_order_line_id}"

    # Headers for the API request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    # Data to send in the request body
    data = {}

    if batch_id is not None:
        data["batchId"] = batch_id
    else:
        data["batchId"] = None  # Set to null in JSON

    data["handlingRank"] = handling_rank

    try:
        # Make the PUT request
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        return response.json()  # Return the JSON response as a dictionary
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error updating work order line: {e}")
        return None
    
def loadDF(datestamp, timeStamp):
    
    df = pd.read_csv(f'Batches_Archive/abs_toBatchInPPG_{datestamp}_{timeStamp}.csv')

    return df

def batchAndAllocateOrders(datestamp, timeStamp):
    
    df = loadDF(datestamp, timeStamp)
    #create a blan ordersBatched column
    df['ordersBatched'] = ""
    df['soCount'] = 0
    print(df)

    #loop through each df 
    for index, row in df.iterrows():
        print(row['batchID'])
        print(row['PPG_BatchName'])
        print(row['DocNums'])

        batchName = row['PPG_BatchName']
        soArr = row['DocNums'].split(', ')
        
        #create allocated arr to Store allocated orders
        allocatedArr = []
        batchCreated = False

        for soNum in soArr:
            print(soNum)
            id = get_orders(soNumber=soNum)
            print(id)
            if id is not None:
                isAllocated, _ = check_if_orderIsAllocated(id)
            
                if isAllocated:
                    allocatedArr.append(soNum)
                else:
                    allocate_order(id)
                    isAllocated, _ = check_if_orderIsAllocated(id)
                    if isAllocated:
                        allocatedArr.append(soNum)

                if isAllocated:
                    if len(allocatedArr) > 0:
                        if not batchCreated:
                            x = create_batch(batchName)
                            if x is not None:
                                if x['batch']['name'] == batchName:
                                    batchCreated = True
                                    print("Batch Created")

                        batchID = get_batches(batchName)

                        workOrderLines = get_work_order_lines(id)
                        workOrderLineID = []

                        if workOrderLines is not None:
                            for line in workOrderLines:
                                workOrderLineID.append(line['id'])

                            for line in workOrderLineID:
                                x = update_work_order_line(line, batchID, handling_rank=len(allocatedArr ))

                            workOrderLines = get_work_order_lines(id)
                            for line in workOrderLines:
                                #check if the batchID in the line matches the batchID
                                if line['batchId'] != batchID:
                                    allocatedArr.pop()
                                    break 
                        else:
                            #pop out the sales order from allocated arr
                            allocatedArr.pop()

        #concatenate allocatedArr into a string and store in a new daataframe column for this wor
        df.at[index, 'ordersBatched'] = ', '.join(allocatedArr)
        df.at[index, 'soCount'] = len(allocatedArr)
        df.to_csv(f'BatchToPrint_Log/abs_toPrintInSAP_{datestamp}_{timeStamp}.csv', index=False)


    #output df to a new csv file in BatchToPrint_Log
    df.to_csv(f'BatchToPrint_Log/abs_toPrintInSAP_{datestamp}_{timeStamp}.csv', index=False)

    return df

def returnOrderLines(order_id = None, orderJson=None, returnJSONORDF= False):
    if order_id is None and orderJson is None:
        return None
    if orderJson is None:
        orderJson = get_order_by_id(order_id)

    #check if order is in order json
    if returnJSONORDF:
        return orderJson['order']['order_lines']
    else:
        #convert to a DF
        df_orderLines = pd.DataFrame(orderJson['order']['order_lines'])
        return df_orderLines
    
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

def get_order_lines(order_lineid: str) -> Optional[Dict]:
    """
    Fetches order lines for a specific order ID from the ItemPath API.
    
    Args:
        order_id (str): The ID of the order to retrieve order lines for.
        
    Returns:
        dict: The JSON response from the API as a Python dictionary, or None if an error occurs.
    """
    # Load variables from the environment
    base_url = os.getenv("ITEMPATH_URL")  # e.g., http://subdomain.itempath.com or IP
    auth_token = os.getenv("ITEMPATH_APPLICATIONTOKEN")  # Bearer token for authentication
    
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_TOKEN must be set in the .env file")
    
    # Construct the full URL
    url = f"{base_url}/api/order_lines/{order_lineid}"
    
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
        print(f"Error fetching order lines: {e}")
        return None

def main():
    
    load_dotenv()

    so = "1112571"
    order_id = get_orders(soNumber=so)

    order = get_order_by_id(order_id)

    print(allocate_order(order_id, allocateOrDeallocate=False))


    return
    orderLines = returnOrderLines(orderJson=order)

    print(orderLines)

    isOrderAllocated = check_if_orderIsAllocated(order_id)

    if isOrderAllocated:
        #deallocate order
        allocate_order(order_id, allocateOrDeallocate=False)
        time.sleep(2)
    else:
        return
    
    isOrderAllocated, _ = check_if_orderIsAllocated(order_id)
    print(isOrderAllocated)

    orderLines = returnOrderLines(orderJson=order)
    print(orderLines)

    jerseyPickingStorageUnits = [{"name": "G1"},{"name": "G2"},{"name": "G3"},{"name": "G4"}]
    lnPickingStorageUnits = [{"name": "V1"},{"name": "V2"},{"name": "V3"},{"name": "V4"},{"name": "V5"}]
        
    #iterate through the orderlines
    for index, row in orderLines.iterrows():
        #get the batch id
        materialName = row['materialName']

        materialDetail = get_materials(materialName)
        print(materialDetail)

        #check if 'materials': got returned in materialDetail
        if 'materials' in materialDetail:
            groupCode = materialDetail['materials'][0]['GroupCode']
        else:
            groupCode = None

        print(groupCode)
        if groupCode == '121':
            storageUnits = jerseyPickingStorageUnits
        elif groupCode == '112':
            storageUnits = lnPickingStorageUnits
        else:
            storageUnits = None

        if storageUnits is not None:
            print(get_order_lines(row['id']))
            x = update_order_line_storageUnits(row['id'], storageUnits)
            print(x)

        #if groupCode is not None


    #store orderlines in a temp csv
    orderLines.to_csv('orderLines.csv', index=False)
    return
    load_dotenv()

    datestamp = "20241127"
    timeStamp = "193026"

    outputDf = batchAndAllocateOrders(datestamp, timeStamp)
    
    return


if __name__ == "__main__":
    main()