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

#### Next, created a cluster on Databricks to clean the data

![image](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/d3bec02d-9a8e-40a9-9aed-8987925ed2e9)

#### Imported the config parser to load the keys required to connect from Databricks to Azure Data Lake Gen 2

![WhatsApp Image 2024-04-29 at 22 34 37_52690bdf](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/d3938c38-975e-4f5c-8d93-f2f630587951)

#### Used the above keys to mount the data into DBFS (Databricks File System) for cleaning

![WhatsApp Image 2024-04-29 at 22 36 06_90a81e8e](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/b49f347b-38bc-4d09-8948-a1852feec8b1)

#### Viewed the mount points

![WhatsApp Image 2024-04-29 at 22 36 40_3abec670](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/8ab062f2-dcf8-43a3-bb2e-66d3f50f6753)

#### Loaded and cleaned the data

![WhatsApp Image 2024-05-01 at 17 56 03_952b4b90](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/eaee9a74-7d0e-49e5-a8ae-04e4582a6aae)

![WhatsApp Image 2024-05-01 at 17 58 00_60115df5](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/83e99362-10b2-45f0-9aa6-794f3c753e65)

![WhatsApp Image 2024-05-01 at 17 59 11_cf037b56](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/e74a40d5-ac67-4259-9afc-830f35a642dd)

![WhatsApp Image 2024-05-01 at 18 19 23_7557d0f7](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/388fea18-331d-4bff-a767-152b02e36994)

![WhatsApp Image 2024-05-01 at 18 23 17_5cdb055b](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/7253b8f2-caaa-4cc2-88b0-b58d565c16a8)

![WhatsApp Image 2024-05-01 at 18 24 36_310a6c89](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/32de6005-44fe-453a-998e-749a5d618fd2)

![WhatsApp Image 2024-05-01 at 18 25 45_b4125847](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/6dc43bc6-4af0-4620-9d98-41e0f64af95d)

![WhatsApp Image 2024-05-01 at 18 26 38_59d6285b](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/b6e39c6a-642e-4a50-b7ce-ff317b7f0098)











