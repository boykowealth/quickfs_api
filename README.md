
# Financial Statements Data Retrieval with QuickFS API

This repository provides a Python-based solution for retrieving financial data (quarterly and annual) using the QuickFS API. The solution offers methods for fetching and processing company financial statements efficiently, including downcasting data to optimize memory usage.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Class Methods](#class-methods)
  - [Executing the Script](#executing-the-script)
- [Example](#example)
- [License](#license)

## Installation

To run this project, you'll need to install the following dependencies:

```bash
pip install pandas requests
```

Make sure to replace `api` in the script with your QuickFS API key.

## Configuration

### API Key
Replace `#######` with your QuickFS API key:
```python
global api
api = '#######'
```

### Dependencies
The script requires `pandas` and `requests` for data manipulation and API interaction, respectively. You can install these packages as shown above.

## Usage

### Class Methods

The main functionality is contained within the `FS` class, which includes several static methods for data retrieval and processing:

1. **`FS.quarterly(ticker)`**: Fetches quarterly financial data for a specific ticker.
2. **`FS.annual(ticker)`**: Fetches annual financial data for a specific ticker.
3. **`FS.company_list()`**: Reads the company list from a Feather file specified in the script.
4. **`FS.downcast_df(df)`**: Downcasts the DataFrame to optimize memory usage by reducing integer and float sizes.
5. **`FS.db_quarterly()`**: Iterates over the company list, fetches quarterly data for each company, and combines it into a single DataFrame.
6. **`FS.db_annual()`**: Iterates over the company list, fetches annual data for each company, and combines it into a single DataFrame.
7. **`FS.main()`**: The main method that executes the quarterly and annual data retrieval, saving results to Parquet files.

### Executing the Script

The script can be run by calling the `FS.main()` method, which handles the entire workflow:

1. Loads the company list.
2. Retrieves quarterly and annual financial data for each company.
3. Saves the results as Parquet files.

### Example

Here’s a quick example of how to use the `FS` class to retrieve and save financial data:

```python
from financial_statements import FS

# Run the main method to retrieve and save data
FS.main()
```

## Additional Details

### API Integration Classes

The `qfs_q` and `qfs_y` classes (in `API_QFS.py`) handle the API requests to QuickFS for quarterly and annual data respectively, using connection pooling for efficiency. They also:
- Construct endpoints dynamically based on the `all_data` flag.
- Validate query parameters.
- Handle API responses and extract financial data to a DataFrame format.
- Optimize memory usage by downcasting data types in the DataFrame.

### Data Processing

To optimize memory usage, the `downcast_df` method downcasts integer and float data types in the combined results DataFrame before saving as Parquet files. This helps reduce storage requirements, especially when handling large datasets.


## Boyko Wealth Financial Data Pipeline
This codebase is part of Boyko Wealth’s advanced financial data infrastructure, designed to streamline the retrieval, processing, and analysis of comprehensive financial data for insightful investment analysis. Our pipeline integrates QuickFS API data with optimized data handling techniques to provide scalable access to quarterly and annual financial statements, leveraging downcasting and memory-efficient methods for handling large datasets.

### Key Features of This Repository
Automated Financial Data Retrieval: Seamlessly pull quarterly and annual financial data across multiple companies through the QuickFS API, eliminating the need for manual data entry.
+ Efficient Memory Management: The data pipeline includes downcasting methods to reduce memory usage, enabling Boyko Wealth to handle large datasets without compromising on performance.
+ Dynamic Data Integration: Financial data is pulled directly into a central processing system, then stored in a Parquet format for easy access and integration with additional analytical tools.
+ Scalable Data Solutions: This codebase provides a foundation for scaling Boyko Wealth’s financial insights, allowing for rapid expansion to additional data sources and analytical features.


