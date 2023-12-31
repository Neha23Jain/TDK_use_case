# TDK_use_case
# ETL Pipeline - README

This project implements an ETL (Extract, Transform, Load) pipeline to process and load point-of-purchase data into an Oracle database, maintaining only the latest records for each customer and product combination.

## Business Objective

The business objective of this pipeline is to process daily updated data containing customer buying behavior, budget information, bookings, and future purchase requirements. The goal is to make this data available in an Oracle database, while ensuring that only the latest information for each customer and product is maintained.

## Pipeline Overview

The ETL pipeline consists of the following steps:

1. **Data Extraction**: Read input data from Parquet files using PySpark.
2. **Data Transformation**:
   - Replace null values in primary key columns (`ovs_cust_name`, `sap_customer_no`, `item_name`, `item_code`) with default values.
   - Order the data by execution timestamp in descending order within each unique combination of primary key columns.
   - Assign row numbers to each row within each unique combination of primary key columns based on the ordering.
   - Select the rows with row number = 1, representing the latest records for each customer and product combination.
   - Typecast the columns to the appropriate data types for the destination Oracle database.
3. **Data Loading**: Connect to the Oracle database and write the transformed data to the specified database table.
4. **End of Pipeline**

## Assumptions and Decisions

- The execution timestamp column represents the time when the data is fed into the system. Therefore, the latest data is determined based on the execution timestamp for each customer and product combination.
- The primary key for the data is a composite key consisting of `ovs_cust_name`, `sap_customer_no`, `item_name`, and `item_code`. This key is used to identify and maintain the latest records.
- The pipeline uses PySpark for data processing to leverage its parallel processing capabilities and handle large datasets efficiently.
- The Oracle database connection details (URL, driver, credentials) should be provided in the configuration file.

## Getting Started

To set up and run the pipeline, follow these steps:

1. Install the necessary dependencies (PySpark, Oracle JDBC driver, etc.).
2. Set the configuration parameters in the `config.py` file, including the input file directory, Oracle database connection details, etc.
3. Place the input Parquet files in the specified directory.
4. Run the `tdk_script_main.py` script to execute the pipeline.
5. Monitor the logs generated by the script for progress and potential errors.
6. Verify that the transformed data has been successfully loaded into the Oracle database table.

## Additional Notes

- Make sure you have the required permissions and access rights to read the input files and write data to the Oracle database.
- Based on the incoming data for the future we can schedule deployement of the pieline using other tools like Docker,CICD,Bamboo,Airflow .
- For troubleshooting or further customization, refer to the script code and the documentation of the used technologies (PySpark, Oracle, etc.).

## Contact Information

For any questions or issues regarding this pipeline, please contact nehajain.nj2310@gmail.com.
