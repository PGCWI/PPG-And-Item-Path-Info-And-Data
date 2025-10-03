# api/orders.py
from flask import request, jsonify, current_app
from services.caching import all_orders_cache, count_orders_cache
from . import orders_bp

@orders_bp.route('/all', methods=['GET'])
def get_all_orders():
    """
    Get all orders data with optional filtering
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in all orders API: {e}")
        return {"status": "error", "message": str(e)}, 500
        
@orders_bp.route('/count', methods=['GET'])
def get_count_orders():
    """
    Get count orders data with optional filtering
    """
    try:
        # Use cached data if available
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
        current_app.logger.error(f"Error in count orders API: {e}")
        return {"status": "error", "message": str(e)}, 500