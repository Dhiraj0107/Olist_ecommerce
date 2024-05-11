# Olist e-commerce

This project is a comprehensive end-to-end data engineering solution deployed in Azure. It begins by setting up a storage account within an Azure subscription and loading CSV data into the container using Python. Connectivity between Databricks and Azure Data Lake Gen 2 is established using secret key, client key, and tenant ID, facilitating data extraction into DataFrames for processing with PySpark. After data cleaning, the processed data is stored back into Azure Data Lake Gen 2. In Azure Synapse Analytics workspace, a T-SQL database is created, and external tables are utilized to access data from Azure Data Lake Gen 2 for seamless querying within Synapse Analytics. Using T-SQL, an external fact table is constructed, incorporating data from external dimension tables. The processed data is then loaded into Power BI for visualization, where a dashboard is created to provide insightful analysis using measures and columns derived from the data. This project demonstrates a robust data engineering pipeline leveraging Azure services and tools, offering a structured approach from data ingestion to visualization. The GitHub repository provides detailed documentation and code implementation.

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

![WhatsApp Image 2024-05-01 at 18 27 24_1c6a11d5](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/c6897a0c-b7fd-4412-8e75-aefd75fff543)

![WhatsApp Image 2024-05-01 at 18 28 25_5a17ffe6](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/c63f3640-59c4-4d93-aaea-153c886eba1c)

![WhatsApp Image 2024-05-01 at 18 30 57_6f3ba017](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/b6eb1738-94b9-4e0f-976c-3f450ede2aaa)

![WhatsApp Image 2024-05-01 at 18 31 35_64b8054b](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/28209087-40bf-49a5-ae42-f2f56f5db956)

![WhatsApp Image 2024-05-01 at 23 34 42_9074f079](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/abe24ec7-0183-43d1-a8c0-3513f36f7f9d)

#### Loaded the cleaned tables back to Azure Data Lake Gen 2

![WhatsApp Image 2024-05-01 at 19 00 58_36d84cf2](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/812ff48b-643a-40f0-a95f-e190637cf1f5)

#### Next, created external tables for all dimension tables and using those created external fact table (Full Queries present in Queries.sql)

![WhatsApp Image 2024-05-04 at 22 37 47_604cfe05](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/5a7b5aaa-bc15-40dd-a0c8-bddc246ba2ea)

![WhatsApp Image 2024-05-04 at 22 48 48_9adf0fa4](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/a916b960-15b0-42f0-8093-cf9fc191cc44)

![WhatsApp Image 2024-05-05 at 12 16 59_d9929436](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/5d9b08b3-9ced-4d2f-a771-bb30fc736fc6)

![WhatsApp Image 2024-05-05 at 14 13 50_6944f368](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/e9dd382a-afac-4c20-aac3-5140564c8246)

![WhatsApp Image 2024-05-05 at 14 21 47_ed35291c](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/9fda6d6d-f456-477f-8893-95460b48e5ea)

![WhatsApp Image 2024-05-05 at 14 36 09_6ae19fa8](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/8c47b551-bdea-4e45-8512-29730de4cb83)

![WhatsApp Image 2024-05-05 at 14 49 56_14d3d6d0](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/71993161-2b5d-463e-a5e3-9663ab75da5e)

![WhatsApp Image 2024-05-05 at 14 57 59_c2ba1e0b](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/3f54517c-a495-40c2-a1b9-eb737d530ddd)

![WhatsApp Image 2024-05-05 at 15 05 31_8e064cc4](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/2b66c1f9-a6ae-4266-88c4-37f2e215b0ad)

![WhatsApp Image 2024-05-05 at 16 58 04_f63d1bd1](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/6c053bde-99ae-450c-bed3-48e00021c12f)

After creating the external table for factOlist, the data was segregated into multiple files so in order to make it easily reusable in Power BI I have consolidated all the data into a single file using Python (Consolidate_factOlist_Files.ipynb).

#### After this, I made a dashboard in Power BI

![WhatsApp Image 2024-05-10 at 22 38 59_8aaaf7f6](https://github.com/Dhiraj0107/Olist_ecommerce/assets/118677714/bda30923-7e73-47bb-bbe7-bfa8de1ca547)

DAX and M Language code used for the above dashboard is present in the "Power BI codes.txt" file

#### Results Found


