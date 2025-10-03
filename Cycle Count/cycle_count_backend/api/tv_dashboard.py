# api/tv_dashboard.py
from flask import jsonify, current_app
from datetime import datetime, timedelta
import pandas as pd
from services.caching import all_transactions_cache
from . import tv_dashboard_bp

@tv_dashboard_bp.route('/today-transactions', methods=['GET'])
def get_today_transactions():
    """
    Get today's transactions optimized for TV dashboard
    """
    try:
        # Use cached data if available
        if all_transactions_cache['data'] is None or all_transactions_cache['data'].empty:
            return {"status": "error", "message": "No transaction data available"}, 500
            
        df = all_transactions_cache['data'].copy()
        
        # Get today's date in YYYY-MM-DD format
        today = datetime.now().strftime('%Y-%m-%d')
        
        # DEBUG: Log the date format and data
        current_app.logger.info(f"TV Dashboard: Looking for transactions on {today}")
        
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
            current_app.logger.warning(f"TV Dashboard: Error in date parsing: {e}, falling back to string comparison")
            # Fallback to simple string comparison
            today_data = df[df['creationDate'].str.startswith(today)]
        
        # DEBUG: Log filtering results
        current_app.logger.info(f"TV Dashboard: Found {len(today_data)} transactions for today")
        
        # If no data for today, return empty but successful response
        if today_data.empty:
            # For debugging, try getting all recent transactions
            recent_days = 3
            min_date = (datetime.now() - timedelta(days=recent_days)).strftime('%Y-%m-%d')
            recent_data = df[df['creationDate'] >= min_date]
            current_app.logger.info(f"TV Dashboard: Found {len(recent_data)} transactions from the last {recent_days} days")
            
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
        current_app.logger.error(f"Error in TV dashboard today transactions API: {e}")
        return {"status": "error", "message": str(e)}, 500

@tv_dashboard_bp.route('/today-stats', methods=['GET'])
def get_today_stats():
    """
    Get transaction statistics for today only
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in TV dashboard today stats API: {e}")
        return {"status": "error", "message": str(e)}, 500