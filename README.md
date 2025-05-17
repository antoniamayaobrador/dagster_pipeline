# Weather Data Pipeline with Dagster and dbt

A data pipeline that extracts weather data using Airbyte, transforms it with dbt, and orchestrates the workflow with Dagster.

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
- Transforms raw data into analytics-ready models
- Located in `weather_project/models/`
- Includes staging and marts layers

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

For production deployment, you can use Dagster's cloud offering or deploy to your own infrastructure using the Dagster Helm chart.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
