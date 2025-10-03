# api/cycle_count.py
from flask import request, jsonify, current_app
from datetime import datetime, timedelta
from cycle_count.logic import run_cycle_count
from cycle_count.sql_query import get_transactions_df
from cycle_count.coverage_analysis import get_coverage_since_date
from . import cycle_count_bp

@cycle_count_bp.route('/run', methods=['POST'])
def trigger_cycle_count():
    """
    Manually trigger a cycle count run. 
    Returns JSON indicating success or error.
    """
    try:
        # Get parameters from request
        data = request.json
        num_orders_jerseys = data.get('NUM_JERSEYCOUNTORDERS', 200)
        num_orders_components = data.get('NUM_COMPONENTCOUNTORDERS',0)

        # change num_orders to an int if possible and if a string
        if isinstance(num_orders_jerseys, str):
            num_orders_jerseys = int(num_orders_jerseys)

        opt_storage_units = data.get('optStorageUnits', [])
        opt_qualifiers = data.get('optQualifers', [])
        opt_locations = data.get('optLocations', [])
        additionalPrefix = data.get('additionalPrefix', "")
        
        created_orders = run_cycle_count(
            api_base_url=current_app.config['ITEMPATH_API_URL'], 
            access_token=current_app.config['ITEMPATH_API_KEY'],
            NUM_JERSEYCOUNTORDERS=num_orders_jerseys,
            NUM_COMPONENTCOUNTORDERS=num_orders_components,
            optStorageUnits=opt_storage_units, 
            optQualifers=opt_qualifiers,
            opt_locations=opt_locations,
            additionalPrefix=additionalPrefix
        )

        created_orders.to_csv("tempCreatedOrders.csv", index=False)
        
        current_app.logger.info(f"Manual cycle count triggered. Orders created: {len(created_orders)}")
        return {"status": "success", "orders_created": len(created_orders)}, 200
    except Exception as e:
        current_app.logger.error(f"Error in manual cycle count trigger: {e}")
        return {"status": "error", "message": str(e)}, 500

@cycle_count_bp.route('/get-transactions', methods=['GET'])
def get_transactions_manually():
    """
    Get the count transactions from the database.
    """
    try:
        min_date = request.args.get('minDate', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        
        x = get_transactions_df(
            transaction_type="[or]6",  # OrderCount (type 6)
            creation_date_from=min_date
        )
        
        x.to_csv("tempTransactions.csv", index=False)
        return {"status": "success", "message": f"Transactions retrieved: {len(x)} records"}, 200
    except Exception as e:
        current_app.logger.error(f"Error in getting transactions: {e}")
        return {"status": "error", "message": str(e)}, 500
    
# Add this new route to your existing cycle_count.py file
@cycle_count_bp.route('/coverage', methods=['GET'])
def get_cycle_count_coverage():
    """
    Get cycle count coverage statistics since a specified date
    
    Query Parameters:
        date: Reference date in YYYY-MM-DD format (optional, defaults to 60 days ago)
    
    Returns:
        JSON with coverage data and summary statistics
    """
    date_str = request.args.get('date', None)
    
    # Get coverage data
    result = get_coverage_since_date(date_str)
    
    return jsonify(result)