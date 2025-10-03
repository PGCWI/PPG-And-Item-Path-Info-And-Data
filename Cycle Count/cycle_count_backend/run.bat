@echo off

REM Create virtual environment if not exist
if not exist venv (
    python -m venv venv
)

REM Activate the virtual environment
call venv\Scripts\activate

REM Install required packages
pip install --upgrade pip
pip install -r requirements.txt

REM Set environment variables (optional)
set FLASK_ENV=development

REM Run the Flask application
REM python app.py

REM Pause at end
pause
