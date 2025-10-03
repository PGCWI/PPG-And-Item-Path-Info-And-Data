# app.py
import logging
import os
from flask import Flask
from flask_cors import CORS
from config import Config
from dotenv import load_dotenv

# Import API modules
from api import init_app
from services.data_refresh import init_data_refresh_schedulers

def create_app(config_class=Config):
    """
    Application factory pattern to create the Flask app
    """
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes
    
    # Load default/base config
    app.config.from_object(config_class)
    
    # Setup logging
    logging.basicConfig(
        filename=app.config['LOG_FILE'],
        level=app.config['LOG_LEVEL'],
        format='%(asctime)s %(levelname)s: %(message)s'
    )
    app.logger.addHandler(logging.StreamHandler())
    app.logger.setLevel(app.config['LOG_LEVEL'])
    
    # Register API blueprints
    init_app(app)
    
    # Initialize background tasks
    init_data_refresh_schedulers(app)
    
    return app

def main():
    """
    Main entry point for running the Flask app.
    """
    print("STARTING")
    load_dotenv()  # Load environment variables
    
    app = create_app()
    
    # Start the Flask development server
    app.run(
        host='0.0.0.0',  # Always use 0.0.0.0 for binding
        port=int(os.getenv('PORT', 5099)),
        debug=os.getenv('DEBUG', 'False').lower() == 'true'
    )

if __name__ == '__main__':
    main()