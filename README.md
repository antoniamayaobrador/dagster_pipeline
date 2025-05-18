# Weather Data Pipeline with Dagster and dbt

A modern data pipeline that extracts weather data using Airbyte, transforms it with dbt, and orchestrates the workflow with Dagster. This pipeline collects current weather conditions and forecasts, processes them through a series of transformations, and produces analytics-ready data models for weather analysis and monitoring.

## Overview

This pipeline follows a modern data stack approach:

1. **Data Extraction (Airbyte)**
   - Collects current weather conditions
   - Retrieves daily and hourly weather forecasts
   - Handles API rate limiting and data freshness

2. **Data Transformation (dbt)**
   - Staging layer for raw data normalization
   - Marts layer for business-ready analytics
   - Includes data quality tests and documentation

3. **Orchestration (Dagster)**
   - Manages the end-to-end data pipeline
   - Provides asset-based orchestration
   - Handles dependencies and scheduling

## Project Structure

```
weather/
├── weather/                      # Python package
│   ├── __init__.py
│   ├── airbyte.py               # Airbyte asset definitions
│   ├── airbyte_manual_asset.py  # Manual Airbyte asset configurations
│   ├── constants.py             # Project constants and configurations
│   ├── dbt.py                   # dbt asset definitions
│   ├── definitions.py           # Dagster definitions
│   ├── project.py               # dbt project configuration
│   └── schedules.py             # Pipeline schedules
├── weather_project/             # dbt project
│   ├── dbt_project.yml          # dbt project configuration
│   ├── models/                  # dbt models
│   ├── seeds/                   # Seed data
│   └── tests/                   # dbt tests
└── README.md                    # This file
```

## Prerequisites

- Python 3.8+
- Docker (for Airbyte)
- dbt
- Dagster

## Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd dbt_weather_dagster/weather
   ```

2. **Create and activate a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

4. **Set up environment variables**
   Create a `.env` file in the project root with the following variables:
   ```
   AIRBYTE_HOST=13.39.50.171.nip.io
   AIRBYTE_PORT=80
   AIRBYTE_EMAIL=your_email@example.com
   AIRBYTE_PASSWORD=your_password
   AIRBYTE_CONNECTION_ID=your_connection_id
   ```

## Running the Pipeline

### Start the Dagster UI
```bash
dagster dev
```

Then navigate to `http://localhost:3000` to access the Dagster UI.

### Run the pipeline manually
```bash
python -m weather.definitions
```

## Components

### Airbyte Integration
- Extracts weather data from various sources
- Configured in `airbyte.py` and `airbyte_manual_asset.py`
- Uses the Airbyte API to trigger syncs

### dbt Transformations

The dbt project consists of two main layers:

#### Staging Layer (`models/staging/`)
- `stg_weather_current`: Current weather conditions
- `stg_weather_forecast_daily`: Daily weather forecasts
- `stg_weather_forecast_hourly`: Hourly weather forecasts

#### Marts Layer (`models/marts/`)
- `weather_current_metrics`: Analyzed current weather data
- `weather_daily_snapshot`: Point-in-time snapshots of daily forecasts
- `weather_daily_summary`: Aggregated daily weather statistics
- `weather_extremes`: Analysis of extreme weather conditions
- `weather_trends`: Long-term weather pattern analysis

Each model is documented and includes data quality tests to ensure reliability.

### Schedules
- Configured in `schedules.py`
- Default schedule: Daily at 8 AM (Europe/Madrid timezone)

## Development

### Running Tests
```bash
pytest
```

### Code Formatting
```bash
black .
```

### Linting
```bash
flake8
```

## Deployment

### Local Development
1. Start the Dagster UI:
   ```bash
   dagster dev
   ```
2. Access the UI at `http://localhost:3000`
3. Run the pipeline manually or set up schedules

### Dagster Cloud Deployment
1. Configure your Dagster Cloud account
2. Set up the following environment variables:
   - `DAGSTER_CLOUD_API_TOKEN`
   - `ORGANIZATION_ID`
   - `SNOWFLAKE_DATABASE`
   - Other required environment variables

3. Deploy using GitHub Actions:
   - Push changes to trigger automatic deployment
   - Monitor deployment status in GitHub Actions
   - View logs and assets in Dagster Cloud UI

### Infrastructure Requirements
- Python 3.8+
- Snowflake database
- Airbyte instance
- GitHub repository for CI/CD

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.