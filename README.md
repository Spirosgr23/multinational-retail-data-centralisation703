# Centralized Sales Data System for Multinational Company

## Table of Contents
1. [Project Description](#project-description)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [License Information](#license-information)

## Project Description
This project aimed to centralize various distributed sales data sources of a multinational company into one accessible and analyzable database. The primary goal was to create a system to store the company's sales data in a centralized database, making it the single source of truth for sales-related information. This process involved data extraction, cleaning, and uploading into a PostgreSQL database, ensuring that the data is easily accessible for up-to-date business metrics and analysis. The project encompasses the utilization of Python for data handling, with emphasis on packages such as Pandas for data manipulation, SQLAlchemy for database interaction, and Boto3 for AWS services. Key learnings include data cleaning techniques, database management, and automation of data pipeline processes.

## Installation Instructions
1. **Python Environment**: Ensure Python 3.x is installed.
2. **Dependencies**: Install necessary Python packages including `pandas`, `sqlalchemy`, `pyyaml`, `boto3`, and `requests`.
3. **Database Setup**: Set up a PostgreSQL database with the necessary configurations.
4. **AWS Configuration**: Configure AWS CLI with the necessary permissions to access external S3 bucket.
5. **Clone the Project**: Clone this repository to your local machine.

## Usage Instructions
1. **Setting Up Credentials**: Store your database credentials in `db_creds.yaml` and `my_creds.yaml` files.
2. **Running the Scripts**: Execute `main_code.py` to start the data extraction, cleaning, and uploading process.
3. **Monitoring**: Observe the terminal for process logs and confirm successful data upload.

## File Structure
- `data_cleaning.py`: Contains methods for cleaning various datasets.
- `data_extraction.py`: Includes functions for extracting data from different sources like RDS, S3, and PDFs.
- `database_utils.py`: Utilities for database connection and operations.
- `main_code.py`: Main script orchestrating the data processing pipeline.
- `db_creds.yaml` and `my_creds.yaml`: YAML files containing database credentials.

## License Information
This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
