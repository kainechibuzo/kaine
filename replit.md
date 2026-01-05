# Ultimate Bot

## Overview
A short-term adaptive trading bot with a real-time web dashboard. The bot fetches trading signals from multiple cryptocurrency exchanges and displays momentum, reversal, and range signals through a Flask web interface.

## Project Structure
- `ultimatest_bot.py` - Main application file containing:
  - Flask web server with dashboard UI
  - Trading signal generation logic
  - Multi-exchange data fetching (CoinGecko, Kraken, OKX, KuCoin, etc.)
  - API endpoints for signals

## Running the Application
The application runs on port 5000 and provides:
- Web dashboard at `/` 
- API endpoints:
  - `/api/signals` - All trading signals
  - `/api/signals/<strategy>` - Signals for specific strategy (momentum, reversal, range)
  - `/api/health` - Health check

## Environment Variables
The bot uses various optional environment variables for configuration:
- `API_PORT` - Server port (default: 5000)
- `CYCLE_SECONDS` - Main loop cycle time
- Various confidence thresholds and strategy parameters

## Dependencies
- Python 3.11
- Flask
- Requests
- urllib3

## Recent Changes
- 2026-01-05: Initial Replit environment setup
