# Cycle Counting Backend (Flask + SQLite)

This project is a fully functional Flask-based backend service for **automated cycle counting**. It integrates with the **ItemPath API** to retrieve materials/locations, applies a **FIFO** strategy using `putDate` and `lastCountDate`, and generates **cycle count orders**. A local **SQLite** database stores the history of created count orders. The system also includes:

- Manual triggering via an API endpoint.
- Automated daily execution with APScheduler.
- Robust logging and error handling.

## Features

1. **FIFO-Based Logic**: Prioritizes older (first-in) items for counting first.
2. **ItemPath API Integration**: Fetches items and locations via HTTP requests.
3. **SQLite Storage**: Saves cycle count history in `cycle_counts.db`.
4. **Scheduling**: Runs automatically daily at a configurable time (default 2 AM).
5. **Manual Trigger**: Exposes a `POST /api/cycle-count/run` endpoint to force a run anytime.

## Project Structure

