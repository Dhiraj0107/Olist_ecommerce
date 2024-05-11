# Olist e-commerce end-to-end data engineering project

This project is a comprehensive end-to-end data engineering solution deployed in Azure. It begins by setting up a storage account within an Azure subscription and loading CSV data into it using Python. Connectivity between Databricks and Azure Data Lake Gen 2 is established using secret key, client key, and tenant ID, facilitating data extraction into DataFrames for processing with PySpark. After data cleaning, the processed data is stored back into Azure Data Lake Gen 2. In Azure Synapse Analytics workspace, a T-SQL database is created, and external tables are utilized to access data from Azure Data Lake Gen 2 for seamless querying within Synapse Analytics. Using T-SQL, an external fact table is constructed, incorporating data from external dimension tables. The processed data is then loaded into Power BI for visualization, where a dashboard is created to provide insightful analysis using measures and columns derived from the data. This project demonstrates a robust data engineering pipeline leveraging Azure services and tools, offering a structured approach from data ingestion to visualization. The GitHub repository provides detailed documentation and code implementation.

#### Data Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

#### Created storage account to create containers to store data

![image](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/bf13335b-4420-4187-9c65-e74e982bce0a)

#### Created container and folders inside it to store files after each stage

- raw_data folder was created to add the raw data files from local system which was done by connecting to Azure Data Lake Gen 2 storage using Python (Azure_Data_Engineering_Project.ipynb).
- data-after-etl was created to store the files after ETL using PySpark on Databricks.
- data-after-etl-sql-database was created to store the files after creating external tables to access data in dimension tables and using these created external fact tables with data in this folder.

![image](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/e8772eda-7300-42b1-b827-9d0a408b0be0)

