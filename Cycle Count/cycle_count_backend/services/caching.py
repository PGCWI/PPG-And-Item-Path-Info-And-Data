# services/caching.py
from datetime import datetime

# Global variables to store cached data
count_transactions_cache = {
    'data': None,
    'last_updated': None
}

all_transactions_cache = {
    'data': None,
    'last_updated': None
}

all_orders_cache = {
    'data': None,
    'last_updated': None
}

count_orders_cache = {
    'data': None,
    'last_updated': None
}