# services/data_refresh.py
import logging
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from cycle_count.sql_query import get_transactions_df, get_orders_df
from services.caching import (
    count_transactions_cache, all_transactions_cache,
    all_orders_cache, count_orders_cache
)

def refresh_count_transactions_data(app):
    """
    Refreshes the cycle count transaction data cache every 15 minutes
    """
    try:
        # Get count transactions from the last 30 days
        min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
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

def refresh_all_transactions_data(app):
    """
    Refreshes the general transaction data cache every hour
    """
    try:
        # Get all transactions from the last 30 days
        min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
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

def refresh_all_orders_data(app):
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

def refresh_count_orders_data(app):
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

def init_data_refresh_schedulers(app):
    """
    Initialize the schedulers for refreshing data
    """
    scheduler = BackgroundScheduler()
    # Count transactions - refresh every 15 minutes
    scheduler.add_job(lambda: refresh_count_transactions_data(app), 'interval', minutes=3)
    # All transactions - refresh every hour (less frequent due to potential data size)
    scheduler.add_job(lambda: refresh_all_transactions_data(app), 'interval', minutes=10)
    # New schedules for orders data
    scheduler.add_job(lambda: refresh_all_orders_data(app), 'interval', minutes=10)
    scheduler.add_job(lambda: refresh_count_orders_data(app), 'interval', minutes=5)
    
    scheduler.start()
    
    # Run once at startup
    refresh_count_transactions_data(app)
    refresh_all_transactions_data(app)
    refresh_all_orders_data(app)
    refresh_count_orders_data(app)