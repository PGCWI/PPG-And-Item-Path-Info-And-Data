# api/dashboard.py
from flask import request, jsonify, current_app
from services.caching import count_transactions_cache, all_transactions_cache
from services.analysis import analyze_incorrect_counts, get_incorrect_counts_details
from . import dashboard_bp

@dashboard_bp.route('/count_transactions', methods=['GET'])
def get_dashboard_count_transactions():
    """
    Get count transactions data for the dashboard with optional filtering
    """
    try:
        # Use cached data
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
        current_app.logger.error(f"Error in dashboard count transactions API: {e}")
        return {"status": "error", "message": str(e)}, 500

@dashboard_bp.route('/user-metrics', methods=['GET'])
def get_user_metrics():
    """
    Get aggregated user metrics for the dashboard
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in user metrics API: {e}")
        return {"status": "error", "message": str(e)}, 500

@dashboard_bp.route('/error-report', methods=['GET'])
def get_error_report():
    """
    Get error report data for net gainers and losers
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in error report API: {e}")
        return {"status": "error", "message": str(e)}, 500

@dashboard_bp.route('/transactions', methods=['GET'])
def get_all_dashboard_transactions():
    """
    Get all transactions data for the dashboard with optional filtering
    """
    try:
        # Use cached data if available
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
        
        # Convert DataFrame to JSON
        result = df.to_dict(orient='records')
        
        return jsonify({
            "status": "success", 
            "data": result,
            "totalCount": len(df),
            "returnedCount": len(result),
            "lastUpdated": all_transactions_cache['last_updated'].isoformat() if all_transactions_cache['last_updated'] else None
        })
    except Exception as e:
        current_app.logger.error(f"Error in all transactions API: {e}")
        return {"status": "error", "message": str(e)}, 500

@dashboard_bp.route('/transaction-stats', methods=['GET'])
def get_transaction_stats():
    """
    Get transaction statistics broken down by type
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in transaction stats API: {e}")
        return {"status": "error", "message": str(e)}, 500

@dashboard_bp.route('/transaction-types', methods=['GET'])
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

@dashboard_bp.route('/incorrect-counts', methods=['GET'])
def get_incorrect_counts():
    """
    Analyze count transactions to identify incorrect counts
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in incorrect counts API: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}, 500
        
@dashboard_bp.route('/incorrect-counts-details', methods=['GET'])
def get_incorrect_counts_details_api():
    """
    Get detailed information about incorrect counts
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in incorrect counts details API: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}, 500