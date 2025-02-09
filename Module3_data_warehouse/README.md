# Module 3: Data Warehouse and BigQuery
---

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

Important Note:

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 NOT the entire year of data Parquet Files from the New York City Taxi Data found here:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
If you are using orchestration such as Kestra, Mage, Airflow or Prefect etc. do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.


Load Script: You can manually download the parquet files and upload them to your GCS Bucket or you can use the linked script here:
You will simply need to generate a Service Account with GCS Admin Priveleges or be authenticated with the Google SDK and update the bucket name in the script to the name of your bucket
Nothing is fool proof so make sure that all 6 files show in your GCS Bucket before begining.


NOTE: You will need to use the PARQUET option files when creating an External Table

### BIG QUERY SETUP:
Create an external table using the Yellow Taxi Trip Records.
```
CREATE OR REPLACE EXTERNAL TABLE { Google Project ID }.{ Schema Name }.{ Table Name }
OPTIONS (
  format = 'parquet',
  uris = ['gs://dezoomcamp_2025_wing/yellow_tripdata_2024-*.parquet']
);
```

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).



1.  What is count of records for the 2024 Yellow Taxi Data?

``` 
SELECT COUNT(*) FROM { Google Project ID }.{ Schema Name }.{ Table Name }
```

```Ans : 20,332,093```


2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```Ans : 0 MB for the External Table and 155.12 MB for the Materialized Table ```


3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```Ans : BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.```



4. How many records have a fare_amount of 0?

```Ans : 8,333```

SQL query to collect the number of trips that is free
```
SELECT COUNT(fare_amount) FROM `dezoomcamp-nytaxi-wing.de_zoomcamp_module3.M_yellow_taxi_2024_tripdata`
WHERE fare_amount = 0
```


5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

```Ans : Partition by tpep_dropoff_datetime and Cluster on VendorID```

To create a new table:
```
CREATE OR REPLACE TABLE `{ Google Project ID }.{ Schema Name }.{ Table Name (partition)}`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `{ Google Project ID }.{ Schema Name }.{ Table Name }`
```


6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?


```Ans : 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table```


```
SELECT DISTINCT(VendorID)
FROM `d{ Google Project ID }.{ Schema Name }.{ Table Name (partition or materialized table)}`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```


7. Where is the data stored in the External Table you created?

```Ans : GCP Bucket```



8. It is best practice in Big Query to always cluster your data:


```Ans : False```


9. No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```
Ans : 0 Bytes, Since SELECT COUNT(*) is a metadata operation. The count is pre-computed and store so BigQuery does not need to scan any data, which results in 0 bytes read.
```