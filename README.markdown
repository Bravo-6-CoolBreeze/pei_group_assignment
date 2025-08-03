# PEI Group Assignment

This repository contains the solution and code for the PEI Group Interview Assignment, implemented using Databricks notebooks. The project focuses on data ingestion, transformation, and testing for an e-commerce dataset, leveraging PySpark and Delta Lake for scalable data processing.

## Project Overview

The solution processes e-commerce data (customers, orders, and products) through a layered architecture:
- **Bronze Layer**: Raw data ingestion from Excel, JSON, and CSV files.
- **Silver Layer**: Cleaned and transformed data with consistent data types and formats.
- **Gold Layer**: Enriched and aggregated tables for analysis, including profit calculations.

The implementation is designed to run on Databricks (Free or Paid Edition) and includes unit tests to ensure code reliability.

## Assumptions

- **Development Environment**: The solution was developed on Databricks notebooks, as specified in the assignment.
- **Databricks Free Edition**: Databricks Free Edition (not previous Community Edition) was used, which has some limitations. The code is compatible with both Free and Paid editions.
- **Data Quality**: The datasets are assumed to have no duplicate records. If duplicates exist, additional test cases and data cleaning logic (e.g., deduplication) would be required, pending further clarification on handling duplicates.

## Repository Structure

- **`databricks_notebooks/`**: Contains all Databricks notebooks for setup, processing, and testing.
  - `databricks_data_objects_setup.ipynb`: Sets up the Databricks catalog, schemas, and volume required for the solution.
  - `sales_data_processing_main.ipynb`: Main notebook for data ingestion, transformation, and loading into Bronze, Silver, and Gold layers.
  - `tests_sales_data_processing.ipynb`: Contains unit tests to validate the solution.
  - `sales_data_processing_helper.ipynb`: Helper notebook with reusable functions for ingestion and transformation, imported by other notebooks.

## Prerequisites

- **Databricks Environment**: Access to Databricks (Community Edition or Paid version).
- **Libraries**: Ensure the following Python libraries are installed on your Databricks cluster:
  - `openpyxl`
  - Install via the cluster's library management or run:
    ```bash
    %pip install openpyxl
    ```

## Setup and Execution Instructions

1. **Clone the Repository**:
   - Clone this repository to your local machine or import it into your Databricks workspace:
     ```bash
     git clone https://github.com/<your-username>/pei_group_assignment.git
     ```

2. **Import Notebooks**:
   - Upload the notebooks from the `databricks_notebooks/` folder to your Databricks workspace (via the Databricks UI or Repos feature).

3. **Set Up the Environment**:
   - Run the `databricks_data_objects_setup.ipynb` notebook to create:
     - A Databricks catalog.
     - Schemas for Bronze, Silver, and Gold layers.
     - A volume for storing input files (e.g., `/Volumes/sales/`).
   - **Note**: In Databricks Community Edition, replace volume paths with `/dbfs/tmp/` due to the lack of Unity Catalog support.

4. **Run the Main Pipeline**:
   - Execute the `sales_data_processing_main.ipynb` notebook to:
     - Ingest raw data from Excel, JSON, and CSV files into the Bronze layer.
     - Clean and transform data into the Silver layer.
     - Create enriched and aggregated tables in the Gold layer.

5. **Run Unit Tests**:
   - Execute the `tests_sales_data_processing.ipynb` notebook to run unit tests, which validate:
     - Data ingestion (e.g., correct column names, row counts).
     - Data cleaning (e.g., proper formatting of names, phone numbers, dates).
     - Enrichment and aggregation logic.
   - The tests include edge cases (e.g., invalid files, empty datasets).

6. **Helper Functions**:
   - The `sales_data_processing_helper.ipynb` notebook contains reusable functions imported by the main and test notebooks. Ensure it’s accessible in the workspace (e.g., via `/FileStore/scripts/`).


## Future Improvements

- **Duplicate Handling**: If duplicates are present in the datasets, add deduplication logic (e.g., using `dropDuplicates()` in PySpark) based on specific business rules.
- **Scalability**: Optimize for larger datasets by leveraging Databricks’ distributed computing features.
- **Additional Tests**: Expand test coverage for edge cases like malformed data or schema mismatches.

## Contact

For questions or feedback, please contact up.vivek.up@gmail.com or open an issue on this repository.
