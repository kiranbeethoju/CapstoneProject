# Setup Guide for Azure Big Data Capstone Project

## Prerequisites

- Python 3.8 or higher
- Azure SQL Database access
- Git

## Installation Steps

### 1. Clone the Repository
```bash
git clone https://github.com/kiranbeethoju/CapstoneProject.git
cd CapstoneProject
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the project root with your Azure SQL Database credentials:

```bash
# Azure SQL Database Configuration
AZURE_SQL_SERVER=your_server.database.windows.net
AZURE_SQL_DATABASE=your_database_name
AZURE_SQL_USERNAME=your_username
AZURE_SQL_PASSWORD=your_password
AZURE_SQL_PORT=1433
AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server
AZURE_SQL_ENCRYPT=yes
AZURE_SQL_TRUST_SERVER_CERTIFICATE=yes

# Redis Configuration (optional, for caching)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Application Configuration
SQL_ECHO=False
LOG_LEVEL=INFO
```

### 4. Run the Dashboard
```bash
python3 compact_dashboard.py
```

The dashboard will be available at: http://localhost:5003

### 5. Additional Data Processing Scripts

#### Data Cleaning Script
```bash
python3 "Data-Cleaning 2025-07-21 19_10_49.py"
```

This script provides advanced data cleaning and geospatial processing:
- GPS outlier removal for NYC area
- Coordinate system normalization
- Timezone handling and UTC conversion
- Automatic geometry creation from lat/lon columns

#### Parquet-to-CSV Conversion Script
**Note**: This script is designed for Databricks environment and requires Azure Data Lake Storage configuration.

```python
# Configure in Databricks notebook:
storage_account_name = "your_storage_account_name"
container_name = "your_container_name"
account_key = "your_account_key"  # Use secure configuration
```

## Project Structure

```
├── compact_dashboard.py          # Main dashboard application
├── transportation_etl.py         # ETL pipeline for data processing
├── check_table_structure.py      # Utility to inspect database schema
├── Parquet-to-csv.py             # Databricks data format conversion script
├── Data-Cleaning 2025-07-21 19_10_49.py  # Geospatial data cleaning utility
├── config/
│   ├── azure_database.py         # Azure SQL Database configuration
│   └── database.py               # Database utilities
├── data/
│   ├── processed/                # Processed data files (excluded from git)
│   └── analytics/                # Analytics results (excluded from git)
├── docker/                       # Docker configuration files
├── sql/                          # SQL initialization scripts
├── requirements.txt              # Python dependencies
└── README.md                     # Project documentation
```

## Features

- **Real-time Dashboard**: Single-screen comprehensive transportation analytics
- **Interactive Maps**: Geographic visualization with heatmaps
- **Data Processing**: ETL pipeline for data cleaning and transformation
- **Azure Integration**: Direct connection to Azure SQL Database
- **Caching**: In-memory caching for improved performance

## Troubleshooting

### Connection Issues
- Ensure your Azure SQL Database firewall allows connections from your IP
- Verify credentials in the `.env` file
- Check that the ODBC Driver 18 for SQL Server is installed

### Missing Dependencies
- Run `pip install -r requirements.txt` to install all required packages
- For ODBC issues, install the Microsoft ODBC Driver for SQL Server

### Data Issues
- The ETL pipeline will automatically process data from Azure SQL Database
- Large data files are excluded from the repository to keep it lightweight
- Processed data is generated automatically when running the dashboard

## Security Notes

- Never commit credentials to the repository
- Use environment variables for all sensitive information
- The `.env` file is excluded from git via `.gitignore`
- Large data files are excluded to prevent repository bloat 