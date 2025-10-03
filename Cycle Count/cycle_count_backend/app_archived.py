# app.py
import logging
import json
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
from config import Config
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from cycle_count.scheduler import init_scheduler
from cycle_count.logic import run_cycle_count, run_empty_cycle_count
from dotenv import load_dotenv
import os

# Import the SQL-based replacement functions
from cycle_count.sql_query import get_transactions_df
from cycle_count.sql_query import get_orders_df  # Import the new orders function

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Load default/base config
app.config.from_object(Config)

# Setup logging
logging.basicConfig(
    filename=app.config['LOG_FILE'],
    level=app.config['LOG_LEVEL'],
    format='%(asctime)s %(levelname)s: %(message)s'
)
app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(app.config['LOG_LEVEL'])

# Global variables to store cached data
count_transactions_cache = {
    'data': None,
    'last_updated': None
}

all_transactions_cache = {
    'data': None,
    'last_updated': None
}

# New cache for orders data
all_orders_cache = {
    'data': None,
    'last_updated': None
}

count_orders_cache = {
    'data': None,
    'last_updated': None
}

def refresh_count_transactions_data():
    """
    Refreshes the cycle count transaction data cache every 15 minutes
    """
    try:
        # Get count transactions from the last 30 days
        min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Note: No need to pass API URL and token with SQL-based approach, 
        # but keeping parameters for backward compatibility
        df = get_transactions_df(
            api_base_url=app.config.get('ITEMPATH_API_URL', ''), 
            access_token=app.config.get('ITEMPATH_API_KEY', ''), 
            transaction_type="[or]6",  # OrderCount (type 6)
            creation_date_from=min_date,
            debug=app.config.get('DEBUG', False)
        )
        
        # Save to CSV as before for backward compatibility
        df.to_csv("tempTransactions.csv", index=False)
        
        # Update cache
        count_transactions_cache['data'] = df
        count_transactions_cache['last_updated'] = datetime.now()
        
        app.logger.info(f"Count transaction data refreshed at {count_transactions_cache['last_updated']}, {len(df)} records")
    except Exception as e:
        app.logger.error(f"Error refreshing count transaction data: {e}")

def refresh_all_transactions_data():
    """
    Refreshes the general transaction data cache every hour
    """
    try:
        # Get all transactions from the last 30 days
        min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Note: No need to pass API URL and token with SQL-based approach, 
        # but keeping parameters for backward compatibility
        df = get_transactions_df(
            api_base_url=app.config.get('ITEMPATH_API_URL', ''), 
            access_token=app.config.get('ITEMPATH_API_KEY', ''),
            transaction_type="[or]1;2;3;4;5;6;9;",  # ManualPick, OrderPick, OrderCount
            storageUnitName="[or]G1;G2;G3;G4;SKID-RACKING",
            creation_date_from=min_date,
            debug=app.config.get('DEBUG', False)
        )
        
        # Save to CSV for backup
        df.to_csv("allTransactions.csv", index=False)
        
        # Update cache
        all_transactions_cache['data'] = df
        all_transactions_cache['last_updated'] = datetime.now()
        
        app.logger.info(f"All transaction data refreshed at {all_transactions_cache['last_updated']}, {len(df)} records")
    except Exception as e:
        app.logger.error(f"Error refreshing all transaction data: {e}")

# New function to refresh all orders data
def refresh_all_orders_data():
    """
    Refreshes the all orders data cache every hour
    """
    try:
        # Get all orders from the last 30 days
        min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = get_orders_df(
            api_base_url=app.config.get('ITEMPATH_API_URL', ''), 
            access_token=app.config.get('ITEMPATH_API_KEY', ''),
            creation_date_from=min_date,
            debug=app.config.get('DEBUG', False)
        )
        
        # Save to CSV for backup
        df.to_csv("allOrders.csv", index=False)
        
        # Update cache
        all_orders_cache['data'] = df
        all_orders_cache['last_updated'] = datetime.now()
        
        app.logger.info(f"All orders data refreshed at {all_orders_cache['last_updated']}, {len(df)} records")
    except Exception as e:
        app.logger.error(f"Error refreshing all orders data: {e}")

# New function to refresh count orders data
def refresh_count_orders_data():
    """
    Refreshes the count orders data cache every hour
    """
    try:
        # Get count orders from the last 30 days
        min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = get_orders_df(
            api_base_url=app.config.get('ITEMPATH_API_URL', ''), 
            access_token=app.config.get('ITEMPATH_API_KEY', ''),
            direction_type=5,  # Count orders only
            creation_date_from=min_date,
            debug=app.config.get('DEBUG', False)
        )
        
        # Save to CSV for backup
        df.to_csv("countOrders.csv", index=False)
        
        # Update cache
        count_orders_cache['data'] = df
        count_orders_cache['last_updated'] = datetime.now()
        
        app.logger.info(f"Count orders data refreshed at {count_orders_cache['last_updated']}, {len(df)} records")
    except Exception as e:
        app.logger.error(f"Error refreshing count orders data: {e}")

# Initialize the schedulers for refreshing data
def init_data_refresh_schedulers():
    scheduler = BackgroundScheduler()
    # Count transactions - refresh every 15 minutes
    scheduler.add_job(refresh_count_transactions_data, 'interval', minutes=3)
    # All transactions - refresh every hour (less frequent due to potential data size)
    scheduler.add_job(refresh_all_transactions_data, 'interval', minutes=10)
    # New schedules for orders data
    scheduler.add_job(refresh_all_orders_data, 'interval', minutes=10)
    scheduler.add_job(refresh_count_orders_data, 'interval', minutes=5)
    
    scheduler.start()
    
    # Run once at startup
    refresh_count_transactions_data()
    refresh_all_transactions_data()
    refresh_all_orders_data()
    refresh_count_orders_data()

# Original cycle count endpoints
@app.route('/api/cycle-count/run', methods=['POST'])
def trigger_cycle_count():
    """
    Manually trigger a cycle count run. 
    Returns JSON indicating success or error.
    """
    try:
        # Get parameters from request
        data = request.json
        num_orders = data.get('NUM_JERSEYCOUNTORDERS', 200)

        #change num_orders to an int if possible and if a string
        if isinstance(num_orders, str):
            num_orders = int(num_orders)

        opt_storage_units = data.get('optStorageUnits', [])
        opt_qualifiers = data.get('optQualifers', [])
        opt_locations = data.get('optLocations', [])
        additionalPrefix = data.get('additionalPrefix', "")
        
        # Note: run_cycle_count still depends on the original ItemPath API
        # You might need to update this function too if you want to fully
        # eliminate the ItemPath API dependency
        created_orders = run_cycle_count(
            api_base_url=app.config['ITEMPATH_API_URL'], 
            access_token=app.config['ITEMPATH_API_KEY'],
            NUM_JERSEYCOUNTORDERS=num_orders,
            optStorageUnits=opt_storage_units, 
            optQualifers=opt_qualifiers,
            opt_locations=opt_locations,
            additionalPrefix=additionalPrefix
        )

        created_orders.to_csv("tempCreatedOrders.csv", index=False)
        
        app.logger.info(f"Manual cycle count triggered. Orders created: {len(created_orders)}")
        return {"status": "success", "orders_created": len(created_orders)}, 200
    except Exception as e:
        app.logger.error(f"Error in manual cycle count trigger: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/cycle-count/get-transactions', methods=['GET'])
def get_transactions_manually():
    """
    Get the count transactions from the database.
    """
    try:
        min_date = request.args.get('minDate', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        
        # Note: No need to pass API URL and token with SQL-based approach
        x = get_transactions_df(
            transaction_type="[or]6",  # OrderCount (type 6)
            creation_date_from=min_date
        )
        
        x.to_csv("tempTransactions.csv", index=False)
        return {"status": "success", "message": f"Transactions retrieved: {len(x)} records"}, 200
    except Exception as e:
        app.logger.error(f"Error in getting transactions: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/cycle-count/empty-locations', methods=['POST'])
def count_empty_locations():
    """
    Count empty locations in a storage unit.
    """
    try:
        data = request.json
        storage_unit = data.get('storageUnit')
        
        if not storage_unit:
            return {"status": "error", "message": "Storage unit is required"}, 400
            
        # Note: run_empty_cycle_count still depends on the original ItemPath API
        # You might need to update this function too if you want to fully
        # eliminate the ItemPath API dependency
        run_empty_cycle_count(
            api_base_url=app.config['ITEMPATH_API_URL'], 
            access_token=app.config['ITEMPATH_API_KEY'], 
            storageUnit=storage_unit
        )
        
        app.logger.info(f"Empty location cycle count triggered.")
        return {"status": "success"}, 200
    except Exception as e:
        app.logger.error(f"Error in empty location count trigger: {e}")
        return {"status": "error", "message": str(e)}, 500

# Count Transactions dashboard API endpoints
@app.route('/api/dashboard/count_transactions', methods=['GET'])
def get_dashboard_count_transactions():
    """
    Get count transactions data for the dashboard with optional filtering
    """
    try:
        # Use cached data if available, otherwise refresh
        if count_transactions_cache['data'] is None:
            refresh_count_transactions_data()
            
        if count_transactions_cache['data'] is None or count_transactions_cache['data'].empty:
            return {"status": "error", "message": "No count data available"}, 500
            
        df = count_transactions_cache['data'].copy()
        
        # Apply filters if provided
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        user_name = request.args.get('userName')
        
        # Simple string comparisons since our dates are now in string format
        if start_date:
            df = df[df['creationDate'] >= start_date]
        
        if end_date:
            df = df[df['creationDate'] <= end_date]
            
        if user_name:
            df = df[df['userName'] == user_name]
        
        # Convert DataFrame to JSON
        result = df.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "lastUpdated": count_transactions_cache['last_updated'].isoformat() if count_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in dashboard count transactions API: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/dashboard/user-metrics', methods=['GET'])
def get_user_metrics():
    """
    Get aggregated user metrics for the dashboard
    """
    try:
        # Use cached data if available, otherwise refresh
        if count_transactions_cache['data'] is None:
            refresh_count_transactions_data()
            
        if count_transactions_cache['data'] is None or count_transactions_cache['data'].empty:
            return {"status": "error", "message": "No count data available"}, 500
            
        df = count_transactions_cache['data'].copy()
        
        # Apply filters if provided
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        
        # Simple string comparisons
        if start_date:
            df = df[df['creationDate'] >= start_date]
        
        if end_date:
            df = df[df['creationDate'] <= end_date]
        
        # Group by user and calculate metrics
        user_metrics = df.groupby('userName').agg({
            'id': 'count',  # Count of transactions
            'quantityConfirmed': 'sum'  # Sum of jerseys counted
        }).reset_index()
        
        # Rename columns for clarity
        user_metrics.columns = ['userName', 'transactionCount', 'jerseyCount']
        
        # Convert to dict for JSON response
        result = user_metrics.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "lastUpdated": count_transactions_cache['last_updated'].isoformat() if count_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in user metrics API: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/dashboard/error-report', methods=['GET'])
def get_error_report():
    """
    Get error report data for net gainers and losers
    """
    try:
        # Use cached data if available, otherwise refresh
        if count_transactions_cache['data'] is None:
            refresh_count_transactions_data()
            
        if count_transactions_cache['data'] is None or count_transactions_cache['data'].empty:
            return {"status": "error", "message": "No count data available"}, 500
            
        df = count_transactions_cache['data'].copy()
        
        # Apply filters if provided
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        
        # Simple string comparisons
        if start_date:
            df = df[df['creationDate'] >= start_date]
        
        if end_date:
            df = df[df['creationDate'] <= end_date]
        
        # Group by location, material, and qualification
        error_report = df.groupby(['locationName', 'materialName', 'qualification']).agg({
            'quantityDeviated': 'sum'  # Sum of quantity deviations
        }).reset_index()
        
        # Split into gainers and losers
        net_gainers = error_report[error_report['quantityDeviated'] > 0]
        net_losers = error_report[error_report['quantityDeviated'] < 0]
        
        # Sort by absolute quantity
        net_gainers = net_gainers.sort_values('quantityDeviated', ascending=False)
        net_losers = net_losers.sort_values('quantityDeviated', ascending=True)
        
        # Convert to dict for JSON response
        gainers_result = net_gainers.to_dict(orient='records')
        losers_result = net_losers.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "netGainers": gainers_result,
            "netLosers": losers_result,
            "lastUpdated": count_transactions_cache['last_updated'].isoformat() if count_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in error report API: {e}")
        return {"status": "error", "message": str(e)}, 500

# All Transactions dashboard API endpoints
@app.route('/api/dashboard/transactions', methods=['GET'])
def get_all_dashboard_transactions():
    """
    Get all transactions data for the dashboard with optional filtering
    """
    try:
        # Use cached data if available, otherwise refresh
        if all_transactions_cache['data'] is None:
            refresh_all_transactions_data()
            
        if all_transactions_cache['data'] is None or all_transactions_cache['data'].empty:
            return {"status": "error", "message": "No transaction data available"}, 500
            
        df = all_transactions_cache['data'].copy()
        
        # Apply filters if provided
        filters = {
            'startDate': request.args.get('startDate'),
            'endDate': request.args.get('endDate'),
            'userName': request.args.get('userName'),
            'materialName': request.args.get('materialName'),
            'transactionType': request.args.get('transactionType'),
            'orderName': request.args.get('orderName'),
            'locationName': request.args.get('locationName')
        }
        
        # Apply each filter if it exists - simple string comparisons
        if filters['startDate']:
            df = df[df['creationDate'] >= filters['startDate']]
        
        if filters['endDate']:
            df = df[df['creationDate'] <= filters['endDate']]
            
        if filters['userName']:
            df = df[df['userName'] == filters['userName']]
            
        if filters['materialName']:
            df = df[df['materialName'] == filters['materialName']]
            
        if filters['transactionType'] and filters['transactionType'].isdigit():
            df = df[df['type'] == int(filters['transactionType'])]
            
        if filters['orderName']:
            df = df[df['orderName'] == filters['orderName']]
            
        if filters['locationName']:
            df = df[df['locationName'] == filters['locationName']]
        
        # Convert DataFrame to JSON (limit to 5000 records for performance)
        result = df.to_dict(orient='records') #df.head(5000).to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "totalCount": len(df),
            "returnedCount": len(df),
            "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in all transactions API: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/dashboard/transaction-stats', methods=['GET'])
def get_transaction_stats():
    """
    Get transaction statistics broken down by type
    """
    try:
        # Use cached data if available, otherwise refresh
        if all_transactions_cache['data'] is None:
            refresh_all_transactions_data()
            
        if all_transactions_cache['data'] is None or all_transactions_cache['data'].empty:
            return {"status": "error", "message": "No transaction data available"}, 500
            
        df = all_transactions_cache['data'].copy()
        
        # Apply filters if provided
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        user_name = request.args.get('userName')
        
        # Simple string comparisons
        if start_date:
            df = df[df['creationDate'] >= start_date]
        
        if end_date:
            df = df[df['creationDate'] <= end_date]
            
        if user_name:
            df = df[df['userName'] == user_name]
        
        # Create transaction type mapping for readability
        transaction_types = {
            0: "NotSet",
            1: "ManualPut",
            2: "ManualPick",
            3: "OrderPut",
            4: "OrderPick",
            5: "Transfer",
            6: "OrderCount",
            7: "ContextCount",
            8: "MaterialRename",
            9: "ManualCorrection",
            10: "ContextCorrection",
            11: "CancelRequest",
            12: "Purge",
            13: "Production",
            15: "KitRename"
        }
        
        # Add readable transaction type name
        df['transactionTypeName'] = df['type'].map(lambda x: transaction_types.get(x, f"Unknown({x})"))
        
        # Calculate statistics
        total_count = len(df)
        
        # Group by transaction type
        type_counts = df.groupby(['type', 'transactionTypeName']).size().reset_index(name='count')
        
        # Calculate percentages
        type_counts['percentage'] = (type_counts['count'] / total_count * 100).round(2)
        
        # Get user stats
        user_counts = df.groupby('userName').size().reset_index(name='count')
        user_counts = user_counts.sort_values('count', ascending=False).head(10)
        
        # Get material stats
        material_counts = df.groupby('materialName').size().reset_index(name='count')
        material_counts = material_counts.sort_values('count', ascending=False).head(10)
        
        # Prepare response
        stats = {
            'totalTransactions': total_count,
            'byType': type_counts.to_dict(orient='records'),
            'topUsers': user_counts.to_dict(orient='records'),
            'topMaterials': material_counts.to_dict(orient='records')
        }
        
        return jsonify({
            "status": "success", 
            "data": stats,
            "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in transaction stats API: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/dashboard/transaction-types', methods=['GET'])
def get_transaction_types():
    """
    Return the list of transaction types for filtering
    """
    transaction_types = {
        0: "NotSet",
        1: "ManualPut",
        2: "ManualPick",
        3: "OrderPut",
        4: "OrderPick",
        5: "Transfer",
        6: "OrderCount",
        7: "ContextCount",
        8: "MaterialRename",
        9: "ManualCorrection",
        10: "ContextCorrection",
        11: "CancelRequest",
        12: "Purge",
        13: "Production",
        15: "KitRename"
    }
    
    type_list = [{"id": k, "name": v} for k, v in transaction_types.items()]
    
    return jsonify({
        "status": "success",
        "data": type_list
    })

# New Orders API endpoints
@app.route('/api/orders/all', methods=['GET'])
def get_all_orders():
    """
    Get all orders data with optional filtering
    """
    try:
        # Use cached data if available, otherwise refresh
        if all_orders_cache['data'] is None:
            refresh_all_orders_data()
            
        if all_orders_cache['data'] is None or all_orders_cache['data'].empty:
            return {"status": "error", "message": "No orders data available"}, 500
            
        df = all_orders_cache['data'].copy()
        
        # Apply filters if provided
        filters = {
            'order_name': request.args.get('order_name'),
            'direction_type': request.args.get('direction_type'),
            'warehouse': request.args.get('warehouse'),
            'priority': request.args.get('priority'),
            'deadline_from': request.args.get('deadline_from'),
            'deadline_to': request.args.get('deadline_to'),
            'creation_date_from': request.args.get('creation_date_from'),
            'creation_date_to': request.args.get('creation_date_to'),
            'order_status_type': request.args.get('order_status_type'),
            'special_incomplete': request.args.get('special_incomplete')
        }
        
        # Apply each filter if it exists
        if filters['order_name']:
            df = df[df['MasterorderName'] == filters['order_name']]
            
        if filters['direction_type']:
            # Handle OR logic for direction type
            if "[or]" in filters['direction_type']:
                type_values = [int(t) for t in filters['direction_type'].replace("[or]", "").split(";") if t and t.isdigit()]
                if type_values:
                    df = df[df['DirectionType'].isin(type_values)]
            else:
                try:
                    df = df[df['DirectionType'] == int(filters['direction_type'])]
                except:
                    # If conversion fails, try string comparison
                    df = df[df['DirectionType'] == filters['direction_type']]
                    
        if filters['warehouse']:
            df = df[df['Warehouse'] == filters['warehouse']]
            
        if filters['priority']:
            try:
                df = df[df['Priority'] == int(filters['priority'])]
            except:
                df = df[df['Priority'] == filters['priority']]
            
        if filters['deadline_from']:
            df = df[df['Deadline'] >= filters['deadline_from']]
            
        if filters['deadline_to']:
            df = df[df['Deadline'] <= filters['deadline_to']]
            
        if filters['creation_date_from']:
            df = df[df['Creationdate'] >= filters['creation_date_from']]
            
        if filters['creation_date_to']:
            df = df[df['Creationdate'] <= filters['creation_date_to']]
            
        if filters['order_status_type']:
            # Handle OR logic for order status
            if "[or]" in filters['order_status_type']:
                status_values = [int(s) for s in filters['order_status_type'].replace("[or]", "").split(";") if s and s.isdigit()]
                if status_values:
                    df = df[df['OrderstatusType'].isin(status_values)]
            else:
                try:
                    df = df[df['OrderstatusType'] == int(filters['order_status_type'])]
                except:
                    df = df[df['OrderstatusType'] == filters['order_status_type']]
            
        if filters['special_incomplete'] is not None:
            try:
                spec_inc_val = int(filters['special_incomplete'])
                df = df[df['SpecialIncomplete'] == spec_inc_val]
            except:
                if filters['special_incomplete'].lower() in ('true', 'yes', '1'):
                    df = df[df['SpecialIncomplete'] == 1]
                elif filters['special_incomplete'].lower() in ('false', 'no', '0'):
                    df = df[df['SpecialIncomplete'] == 0]
        
        # Convert DataFrame to JSON
        result = df.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "totalCount": len(df),
            "returnedCount": len(result),
            "lastUpdated": all_orders_cache['last_updated'].isoformat() if all_orders_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in all orders API: {e}")
        return {"status": "error", "message": str(e)}, 500
        
@app.route('/api/orders/count', methods=['GET'])
def get_count_orders():
    """
    Get count orders data with optional filtering
    """
    try:
        # Use cached data if available, otherwise refresh
        if count_orders_cache['data'] is None:
            refresh_count_orders_data()
            
        if count_orders_cache['data'] is None or count_orders_cache['data'].empty:
            return {"status": "error", "message": "No count orders data available"}, 500
            
        df = count_orders_cache['data'].copy()
        
        # Apply filters if provided (same filters as all orders except direction_type which is already 5)
        filters = {
            'order_name': request.args.get('order_name'),
            'warehouse': request.args.get('warehouse'),
            'priority': request.args.get('priority'),
            'deadline_from': request.args.get('deadline_from'),
            'deadline_to': request.args.get('deadline_to'),
            'creation_date_from': request.args.get('creation_date_from'),
            'creation_date_to': request.args.get('creation_date_to'),
            'order_status_type': request.args.get('order_status_type'),
            'special_incomplete': request.args.get('special_incomplete')
        }
        
        # Apply each filter if it exists
        if filters['order_name']:
            df = df[df['MasterorderName'] == filters['order_name']]
            
        if filters['warehouse']:
            df = df[df['Warehouse'] == filters['warehouse']]
            
        if filters['priority']:
            try:
                df = df[df['Priority'] == int(filters['priority'])]
            except:
                df = df[df['Priority'] == filters['priority']]
            
        if filters['deadline_from']:
            df = df[df['Deadline'] >= filters['deadline_from']]
            
        if filters['deadline_to']:
            df = df[df['Deadline'] <= filters['deadline_to']]
            
        if filters['creation_date_from']:
            df = df[df['Creationdate'] >= filters['creation_date_from']]
            
        if filters['creation_date_to']:
            df = df[df['Creationdate'] <= filters['creation_date_to']]
            
        if filters['order_status_type']:
            # Handle OR logic for order status
            if "[or]" in filters['order_status_type']:
                status_values = [int(s) for s in filters['order_status_type'].replace("[or]", "").split(";") if s and s.isdigit()]
                if status_values:
                    df = df[df['OrderstatusType'].isin(status_values)]
            else:
                try:
                    df = df[df['OrderstatusType'] == int(filters['order_status_type'])]
                except:
                    df = df[df['OrderstatusType'] == filters['order_status_type']]
            
        if filters['special_incomplete'] is not None:
            try:
                spec_inc_val = int(filters['special_incomplete'])
                df = df[df['SpecialIncomplete'] == spec_inc_val]
            except:
                if filters['special_incomplete'].lower() in ('true', 'yes', '1'):
                    df = df[df['SpecialIncomplete'] == 1]
                elif filters['special_incomplete'].lower() in ('false', 'no', '0'):
                    df = df[df['SpecialIncomplete'] == 0]
        
        # Convert DataFrame to JSON
        result = df.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "totalCount": len(df),
            "returnedCount": len(result),
            "lastUpdated": count_orders_cache['last_updated'].isoformat() if count_orders_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in count orders API: {e}")
        return {"status": "error", "message": str(e)}, 500

def analyze_incorrect_counts(df, threshold=2):
    """
    Analyzes count transactions to identify incorrect counts
    
    Args:
        df: DataFrame containing count transactions
        threshold: The threshold for acceptable count variance (default: 2)
        
    Returns:
        Dictionary with analysis results
    """
    import pandas as pd
    import numpy as np
    
    # Ensure DataFrame is copied and types are properly inferred
    counts_df = df.copy().infer_objects()
    
    # Convert creation date to datetime for proper sorting/comparison
    counts_df['creationDate'] = pd.to_datetime(counts_df['creationDate'], format='ISO8601')
    
    # Sort by location, material, and creation date
    counts_df = counts_df.sort_values(['locationName', 'materialName', 'qualification', 'creationDate'])
    
    # Add a column for count variance - absolute difference between requested and confirmed
    counts_df['variance'] = counts_df['quantityExpected'] - counts_df['quantityConfirmed']
    counts_df['absoluteVariance'] = np.abs(counts_df['variance'])
    
    # Identify if count is within threshold
    counts_df['withinThreshold'] = counts_df['absoluteVariance'] <= threshold
    
    # Group by location to count how many times each location was counted
    location_counts = counts_df.groupby(['locationName', 'materialName', 'qualification']).size().reset_index(name='timesLocationCounted')
    
    # Merge this information back to the main dataframe
    counts_df = pd.merge(counts_df, location_counts, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Only consider locations that were counted multiple times
    multi_count_df = counts_df[counts_df['timesLocationCounted'] > 1].copy()
    
    # Identify the first count for each location/material combination
    multi_count_df['isFirstCount'] = False
    first_counts = multi_count_df.groupby(['locationName', 'materialName', 'qualification'])['creationDate'].transform('min')
    multi_count_df.loc[multi_count_df['creationDate'] == first_counts, 'isFirstCount'] = True
    
    # For each count, find the next count of the same location/material (if any)
    multi_count_df['nextCountDate'] = pd.NaT
    multi_count_df['nextCountVariance'] = np.nan
    multi_count_df['countCorrect'] = False  # Explicitly set to boolean
    
    # Process one location/material at a time to find next counts
    for (loc, mat, qual), group in multi_count_df.groupby(['locationName', 'materialName', 'qualification']):
        sorted_group = group.sort_values('creationDate')
        indices = sorted_group.index.tolist()
        
        for i in range(len(indices) - 1):
            current_idx = indices[i]
            next_idx = indices[i + 1]
            
            multi_count_df.loc[current_idx, 'nextCountDate'] = sorted_group.loc[next_idx, 'creationDate']
            multi_count_df.loc[current_idx, 'nextCountVariance'] = sorted_group.loc[next_idx, 'variance']
            multi_count_df.loc[current_idx, 'countCorrect'] = sorted_group.loc[next_idx, 'absoluteVariance'] <= threshold
    
    # Use multi_count_df directly 
    result_df = multi_count_df
    
    # Identify locations that passed (at least one count within threshold)
    location_passed = result_df.groupby(['locationName', 'materialName', 'qualification'])['withinThreshold'].any().reset_index()
    location_passed.columns = ['locationName', 'materialName', 'qualification', 'locationPassed']
    
    # Merge back to get locationPassed flag
    result_df = pd.merge(result_df, location_passed, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Filter to only include locations that passed (had at least one count within threshold)
    analysis_df = result_df[result_df['locationPassed']].copy()
    
    # By User - using ORIGINAL full dataframe for total counts
    user_metrics = []
    for user, group in analysis_df.groupby('userName'):
        # Total counts from the ORIGINAL full dataframe
        total_counts = df[df['userName'] == user].shape[0]
        
        # Convert to int to avoid boolean serialization
        incorrect_counts = int(sum((~group['withinThreshold']) & (~group['countCorrect'])))
        user_metrics.append({
            'userName': user,
            'totalCounts': total_counts,
            'incorrectCounts': incorrect_counts,
            'incorrectPercentage': round(incorrect_counts / total_counts * 100, 2) if total_counts > 0 else 0
        })
    
    # By Day - using ORIGINAL full dataframe for total counts
    analysis_df['countDate'] = analysis_df['creationDate'].dt.date
    day_metrics = []
    for day, group in analysis_df.groupby('countDate'):
        # Total counts from the ORIGINAL full dataframe for this date
        total_counts = df[pd.to_datetime(df['creationDate'], format='ISO8601').dt.date == day].shape[0]
        
        # Convert to int to avoid boolean serialization
        incorrect_counts = int(sum((~group['withinThreshold']) & (~group['countCorrect'])))
        day_metrics.append({
            'date': day.strftime('%Y-%m-%d'),
            'totalCounts': total_counts,
            'incorrectCounts': incorrect_counts,
            'incorrectPercentage': round(incorrect_counts / total_counts * 100, 2) if total_counts > 0 else 0
        })
    
    # Overall metrics - using ORIGINAL full dataframe for total counts
    total_analyzed_counts = df.shape[0]
    
    # Convert to int to avoid boolean serialization
    total_incorrect = int(sum((~analysis_df['withinThreshold']) & (~analysis_df['countCorrect'])))
    
    # Return the results
    return {
        'byUser': sorted(user_metrics, key=lambda x: x['incorrectCounts'], reverse=True),
        'byDay': sorted(day_metrics, key=lambda x: x['date']),
        'overall': {
            'totalCounts': total_analyzed_counts,
            'incorrectCounts': total_incorrect,
            'incorrectPercentage': round(total_incorrect / total_analyzed_counts * 100, 2) if total_analyzed_counts > 0 else 0
        }
    }

def get_incorrect_counts_details(df, threshold=2, only_incorrect=False):
    """
    Returns detailed information about incorrect counts
    
    Args:
        df: DataFrame containing count transactions
        threshold: The threshold for acceptable count variance (default: 2)
        only_incorrect: If True, only return incorrect counts
        
    Returns:
        List of count details
    """
    import pandas as pd
    import numpy as np
    import os
    
    # Make a copy to avoid modifying the original
    counts_df = df.copy().infer_objects()
    
    # Convert creation date to datetime for proper sorting/comparison
    counts_df['creationDate'] = pd.to_datetime(counts_df['creationDate'], format='ISO8601')
    
    # Sort by location, material, and creation date
    counts_df = counts_df.sort_values(['locationName', 'materialName', 'qualification', 'creationDate'])
    
    # Add a column for count variance - absolute difference between requested and confirmed
    counts_df['variance'] = counts_df['quantityExpected'] - counts_df['quantityConfirmed']
    counts_df['absoluteVariance'] = np.abs(counts_df['variance'])
    
    # Identify if count is within threshold
    counts_df['withinThreshold'] = counts_df['absoluteVariance'] <= threshold
    
    # Group by location to count how many times each location was counted
    location_counts = counts_df.groupby(['locationName', 'materialName', 'qualification']).size().reset_index(name='timesLocationCounted')
    
    # Merge this information back to the main dataframe
    counts_df = pd.merge(counts_df, location_counts, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Only consider locations that were counted multiple times
    multi_count_df = counts_df[counts_df['timesLocationCounted'] > 1].copy()
    
    # Identify the first count for each location/material combination
    multi_count_df['isFirstCount'] = False
    first_counts = multi_count_df.groupby(['locationName', 'materialName', 'qualification'])['creationDate'].transform('min')
    multi_count_df.loc[multi_count_df['creationDate'] == first_counts, 'isFirstCount'] = True
    
    # For each count, find the next count of the same location/material (if any)
    multi_count_df['nextCountDate'] = pd.NaT
    multi_count_df['nextCountVariance'] = np.nan
    multi_count_df['countCorrect'] = False  # Explicitly set to boolean
    
    # Process one location/material at a time to find next counts
    for (loc, mat, qual), group in multi_count_df.groupby(['locationName', 'materialName', 'qualification']):
        sorted_group = group.sort_values('creationDate')
        indices = sorted_group.index.tolist()
        
        for i in range(len(indices) - 1):
            current_idx = indices[i]
            next_idx = indices[i + 1]
            
            multi_count_df.loc[current_idx, 'nextCountDate'] = sorted_group.loc[next_idx, 'creationDate']
            multi_count_df.loc[current_idx, 'nextCountVariance'] = sorted_group.loc[next_idx, 'variance']
            multi_count_df.loc[current_idx, 'countCorrect'] = sorted_group.loc[next_idx, 'absoluteVariance'] <= threshold
    
    # Identify locations that passed (at least one count within threshold)
    location_passed = multi_count_df.groupby(['locationName', 'materialName', 'qualification'])['withinThreshold'].any().reset_index()
    location_passed.columns = ['locationName', 'materialName', 'qualification', 'locationPassed']
    
    # Merge back to get locationPassed flag
    multi_count_df = pd.merge(multi_count_df, location_passed, on=['locationName', 'materialName', 'qualification'], how='left')
    
    # Filter to only include locations that passed
    analysis_df = multi_count_df[multi_count_df['locationPassed']].copy()
    
    # If only_incorrect is True, filter to only show incorrect counts
    if only_incorrect:
        analysis_df = analysis_df[(~analysis_df['withinThreshold']) & (~analysis_df['countCorrect'])]
    
    # Prepare the detailed output
    details = []
    for _, row in analysis_df.iterrows():
        # Convert boolean values to integers to ensure JSON serializability
        detail = {
            'date': row['creationDate'].isoformat(),
            'userName': row['userName'],
            'locationName': row['locationName'],
            'materialName': row['materialName'],
            'qualification': row['qualification'],
            'qtyRequested': row['quantityExpected'],
            'qtyConfirmed': row['quantityConfirmed'],
            'variance': row['variance'],
            'withinThreshold': int(row['withinThreshold']),  # Convert to integer
            'nextCountDate': row['nextCountDate'].isoformat() if pd.notna(row['nextCountDate']) else None,
            'nextCountVariance': float(row['nextCountVariance']) if pd.notna(row['nextCountVariance']) else None,
            'countCorrect': int(row['countCorrect']) if pd.notna(row['countCorrect']) else None,  # Convert to integer
            'timesLocationCounted': row['timesLocationCounted'],
            'isFirstCount': int(row['isFirstCount']),  # Convert to integer
            'isIncorrect': int(not row['withinThreshold'] and not row['countCorrect'])  # Convert to integer
        }
        details.append(detail)
    
    return details

# Replace the stub implementations with these real implementations
@app.route('/api/dashboard/incorrect-counts', methods=['GET'])
def get_incorrect_counts():
    """
    Analyze count transactions to identify incorrect counts
    """
    try:
        # Use cached data if available, otherwise refresh
        if count_transactions_cache['data'] is None:
            refresh_count_transactions_data()
            
        if count_transactions_cache['data'] is None or count_transactions_cache['data'].empty:
            return {"status": "error", "message": "No count data available"}, 500
            
        df = count_transactions_cache['data'].copy()
        
        # Apply filters if provided
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        user_name = request.args.get('userName')
        
        # Get the variance threshold from request (default to 2)
        variance_threshold = int(request.args.get('threshold', 2))
        
        # Simple string comparisons
        if start_date:
            df = df[df['creationDate'] >= start_date]
        
        if end_date:
            df = df[df['creationDate'] <= end_date]
            
        if user_name:
            df = df[df['userName'] == user_name]
        
        # Run the analysis
        analysis_results = analyze_incorrect_counts(df, threshold=variance_threshold)
        
        return jsonify({
            "status": "success",
            "data": analysis_results,
            "appliedThreshold": variance_threshold,
            "lastUpdated": count_transactions_cache['last_updated'].isoformat() if count_transactions_cache['last_updated'] else None
        })
        
    except Exception as e:
        app.logger.error(f"Error in incorrect counts API: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}, 500
        
@app.route('/api/dashboard/incorrect-counts-details', methods=['GET'])
def get_incorrect_counts_details_api():
    """
    Get detailed information about incorrect counts
    """
    try:
        # Use cached data if available, otherwise refresh
        if count_transactions_cache['data'] is None:
            refresh_count_transactions_data()
            
        if count_transactions_cache['data'] is None or count_transactions_cache['data'].empty:
            return {"status": "error", "message": "No count data available"}, 500
            
        df = count_transactions_cache['data'].copy()
        
        # Apply filters if provided
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        user_name = request.args.get('userName')
        
        # Get the variance threshold from request (default to 2)
        variance_threshold = int(request.args.get('threshold', 2))
        
        # Check if we should only return incorrect counts
        only_incorrect = request.args.get('onlyIncorrect', 'false').lower() == 'true'
        
        # Simple string comparisons
        if start_date:
            df = df[df['creationDate'] >= start_date]
        
        if end_date:
            df = df[df['creationDate'] <= end_date]
            
        if user_name:
            df = df[df['userName'] == user_name]
        
        # Get the detailed counts
        details = get_incorrect_counts_details(df, threshold=variance_threshold, only_incorrect=only_incorrect)
        
        return jsonify({
            "status": "success",
            "data": details,
            "totalCount": len(details),
            "appliedThreshold": variance_threshold,
            "lastUpdated": count_transactions_cache['last_updated'].isoformat() if count_transactions_cache['last_updated'] else None
        })
        
    except Exception as e:
        app.logger.error(f"Error in incorrect counts details API: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}, 500

# TV Dashboard API endpoints
@app.route('/api/tv-dashboard/today-transactions', methods=['GET'])
def get_today_transactions():
    """
    Get today's transactions optimized for TV dashboard
    """
    try:
        # Use cached data if available, otherwise refresh
        if all_transactions_cache['data'] is None:
            refresh_all_transactions_data()
            
        if all_transactions_cache['data'] is None or all_transactions_cache['data'].empty:
            return {"status": "error", "message": "No transaction data available"}, 500
            
        df = all_transactions_cache['data'].copy()
        
        # Get today's date in YYYY-MM-DD format
        today = datetime.now().strftime('%Y-%m-%d')
        
        # DEBUG: Log the date format and data
        app.logger.info(f"TV Dashboard: Looking for transactions on {today}")
        
        # More robust date filtering - try multiple approaches
        try:
            # Try converting strings to datetime objects first
            df['date_parsed'] = pd.to_datetime(df['creationDate'], format='ISO8601').dt.strftime('%Y-%m-%d')
            today_data = df[df['date_parsed'] == today]
            
            # If that yields no results, try the original string approach
            if today_data.empty:
                today_data = df[df['creationDate'].str.startswith(today)]
            
            # Last resort - try matching just the date portion (for ISO format)
            if today_data.empty:
                today_data = df[df['creationDate'].str[:10] == today]
        except Exception as e:
            app.logger.warning(f"TV Dashboard: Error in date parsing: {e}, falling back to string comparison")
            # Fallback to simple string comparison
            today_data = df[df['creationDate'].str.startswith(today)]
        
        # DEBUG: Log filtering results
        app.logger.info(f"TV Dashboard: Found {len(today_data)} transactions for today")
        
        # If no data for today, return empty but successful response
        if today_data.empty:
            # For debugging, try getting all recent transactions
            recent_days = 3
            min_date = (datetime.now() - timedelta(days=recent_days)).strftime('%Y-%m-%d')
            recent_data = df[df['creationDate'] >= min_date]
            app.logger.info(f"TV Dashboard: Found {len(recent_data)} transactions from the last {recent_days} days")
            
            return jsonify({
                "status": "success", 
                "data": [],
                "message": f"No transactions found for today ({today})",
                "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
            })
        
        # Convert DataFrame to JSON
        today_data.to_csv('temp_todayPull.csv')
        result = today_data.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "totalCount": len(result),
            "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in TV dashboard today transactions API: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/api/tv-dashboard/today-stats', methods=['GET'])
def get_today_stats():
    """
    Get transaction statistics for today only
    """
    try:
        # Use cached data if available, otherwise refresh
        if all_transactions_cache['data'] is None:
            refresh_all_transactions_data()
            
        if all_transactions_cache['data'] is None or all_transactions_cache['data'].empty:
            return {"status": "error", "message": "No transaction data available"}, 500
            
        df = all_transactions_cache['data'].copy()
        
        # Get today's date in YYYY-MM-DD format
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Filter for today's data only
        df = df[df['creationDate'].str.startswith(today)]
        
        # If no data for today, return empty stats
        if df.empty:
            return jsonify({
                "status": "success", 
                "data": {
                    "totalTransactions": 0,
                    "byType": [],
                    "topUsers": [],
                    "topMaterials": []
                },
                "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
            })
        
        # Create transaction type mapping for readability
        transaction_types = {
            0: "NotSet",
            1: "ManualPut",
            2: "ManualPick",
            3: "OrderPut",
            4: "OrderPick",
            5: "Transfer",
            6: "OrderCount",
            7: "ContextCount",
            8: "MaterialRename",
            9: "ManualCorrection",
            10: "ContextCorrection",
            11: "CancelRequest",
            12: "Purge",
            13: "Production",
            15: "KitRename"
        }
        
        # Add readable transaction type name
        df['transactionTypeName'] = df['type'].map(lambda x: transaction_types.get(x, f"Unknown({x})"))
        
        # Calculate statistics
        total_count = len(df)
        
        # Group by transaction type
        type_counts = df.groupby(['type', 'transactionTypeName']).size().reset_index(name='count')
        
        # Calculate percentages
        type_counts['percentage'] = (type_counts['count'] / total_count * 100).round(2)
        
        # Get user stats
        user_counts = df.groupby('userName').size().reset_index(name='count')
        user_counts = user_counts.sort_values('count', ascending=False).head(10)
        
        # Get material stats
        material_counts = df.groupby('materialName').size().reset_index(name='count')
        material_counts = material_counts.sort_values('count', ascending=False).head(10)
        
        # Prepare response
        stats = {
            'totalTransactions': total_count,
            'byType': type_counts.to_dict(orient='records'),
            'topUsers': user_counts.to_dict(orient='records'),
            'topMaterials': material_counts.to_dict(orient='records')
        }
        
        return jsonify({
            "status": "success", 
            "data": stats,
            "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        app.logger.error(f"Error in TV dashboard today stats API: {e}")
        return {"status": "error", "message": str(e)}, 500

def main():
    """
    Main entry point for running the Flask app.
    """
    print("STARTING")
    # Initialize data refresh schedulers
    init_data_refresh_schedulers()

    # Start the Flask development server
    app.run(
        host='0.0.0.0',  # Always use 0.0.0.0 for binding
        port=int(os.getenv('PORT', 5099)),
        debug=os.getenv('DEBUG', 'False').lower() == 'true'
    )

if __name__ == '__main__':
    main()