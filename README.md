# Data Pipeline with Apache Airflow

### Author: Ayotunde Oyewole

## Executive Summary

This project demonstrates the use of Apache Airflow to automate a data pipeline for ETL (Extract, Transform, Load) tasks. The pipeline downloads, extracts, and transforms data from a zipped file and schedules these tasks to run daily. This project showcases how to efficiently manage ETL processes with Airflow by using Bash commands to execute tasks.

## Project Overview

The data pipeline consists of eight main tasks, each executed using Bash commands via Airflow's `BashOperator`. The project is organized with each task running in its own separate directory, following best practices for data pipeline management.

### Preliminary Steps

- **Environment Setup**: Apache Airflow is installed on an Ubuntu environment. A folder named `dags` is created for the project, and the main Python script is saved there.
- **DAG Configuration**: The Directed Acyclic Graph (DAG) is defined with relevant variables such as `dag_id`, `schedule`, `start_date`, and retry settings.
- **Development Tools**: Visual Studio Code (VS Code) is used as the development environment for this project.

## Tasks

1. **Directory Creation**: Create necessary directories (`zipped_file`, `unzipped_files`, `data_extract`, `merged_data`, `transformed_data`) using the `mkdir` Bash command.
  
2. **File Download**: Download the zipped file from a provided URL using a global variable in an f-string format for the URL.

3. **File Extraction**: Extract the contents of the downloaded `.tgz` file using the `tar` command.

4. **CSV Extraction**: Extract specific columns from `vehicle-data.csv` using the `cut` command and save the output to `csv_d.csv`.

5. **TSV Extraction**: Extract columns from `tollplaza-data.tsv` and save them to `tsv_d.csv`.

6. **TXT Extraction**: Extract columns from `payment-data.txt` using `awk` and save the output to `fixed_width_d.csv`.

7. **Document Merger**: Merge all CSV files into one using the `paste` command.

8. **Data Transformation**: Transform the `vehicle type` column to uppercase using the `awk` command.

## Execution

- **Task Scheduling**: Tasks 4, 5, and 6 are set to run in parallel. The DAG is executed and monitored through Airflow's web interface.
- **Error Handling**: Log files are reviewed to troubleshoot any errors in the pipeline execution.

## Results

Screenshots of the DAG creation, execution, and task outputs are included in the project documentation.

## Conclusion

This project showcases the ability to create and deploy an automated ETL pipeline using Apache Airflow, highlighting the power and flexibility of Airflow for data engineering tasks.

## Screenshots

- **DAG Dependency Graph**
- **Gantt Chart of DAG Execution**
- **Logs and Error Handling**

## References

- Apache Airflow Documentation: [Airflow](https://airflow.apache.org/docs/)
