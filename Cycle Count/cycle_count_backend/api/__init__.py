# api/__init__.py
from flask import Blueprint

# Create blueprints for different API domains
cycle_count_bp = Blueprint('cycle_count', __name__, url_prefix='/api/cycle-count')
dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')
orders_bp = Blueprint('orders', __name__, url_prefix='/api/orders')
tv_dashboard_bp = Blueprint('tv_dashboard', __name__, url_prefix='/api/tv-dashboard')

# Import routes to register them with blueprints
from . import cycle_count, dashboard, orders, tv_dashboard

def init_app(app):
    """Register all blueprints with the app"""
    app.register_blueprint(cycle_count_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(orders_bp)
    app.register_blueprint(tv_dashboard_bp)