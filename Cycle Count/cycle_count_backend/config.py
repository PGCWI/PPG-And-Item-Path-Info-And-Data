# config.py
import os
import logging

class Config:
    """
    Default/base configuration for the cycle counting backend.
    Adjust values as needed for your environment.
    """

    # Logging
    LOG_FILE = 'cycle_count.log'
    LOG_LEVEL = logging.INFO

    # Flask settings
    DEBUG = True  # Set to False in production

    # SQLite database path

    # ItemPath API settings (placeholder URLs/Keys)
    ITEMPATH_API_URL = "https://silvercrystal.itempath.com/api"  # Adjust to actual
    ITEMPATH_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTczMjQ2NzY1OSwianRpIjoiMzE0YTlkMDAtODU2NC00YzBmLWI3YmItYTEyZTFkNmQyOWQwIiwidHlwZSI6ImFjY2VzcyIsImlkZW50aXR5IjoiQ0hvcm5lciIsIm5iZiI6MTczMjQ2NzY1OX0.DL48tSqN_NP8nNoAwatbpncmsyoxChpk4Mta_qXrL6E'


    # Scheduler settings
    # Example: run daily at 2 AM (see scheduler.py for usage)
    SCHEDULE_HOUR = 2
    SCHEDULE_MINUTE = 0
