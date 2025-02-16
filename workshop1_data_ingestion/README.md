# **Workshop "Data Ingestion with dlt": Homework**

---

## **Dataset & API**

We’ll use **NYC Taxi data** via the same custom API from the workshop:

🔹 **Base API URL:**  
```
https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
```
🔹 **Data format:** Paginated JSON (1,000 records per page).  
🔹 **API Pagination:** Stop when an empty page is returned.

## **Question 1: dlt Version**

1. **Install dlt**:

```
!pip install dlt[duckdb]
```

> Or choose a different bracket—`bigquery`, `redshift`, etc.—if you prefer another primary destination. For this assignment, we’ll still do a quick test with DuckDB.

2. **Check** the version:

```
!dlt --version

output: dlt 1.6.1
```

---

## **Question 2: Define & Run the Pipeline (NYC Taxi API)**

Use dlt to extract all pages of data from the API.

Steps:

1️⃣ Use the `@dlt.resource` decorator to define the API source.

2️⃣ Implement automatic pagination using dlt's built-in REST client.

3️⃣ Load the extracted data into DuckDB for querying.

```py
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


# define API source
@dlt.resource(name= "rides", write_disposition= "append")

# automatic pagination setup using DLT
def ny_taxi():
  client = RESTClient(
      base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net",
      paginator = PageNumberPaginator(
          base_page = 1, total_path = None
      )
  )

  for page in client.paginate("data_engineering_zoomcamp_api"):
    yield page


# the setup for the pipline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)
```

Load the data into DuckDB to test:
```py
load_info = pipeline.run(ny_taxi)
print(load_info)
```
Start a connection to your database using native `duckdb` connection and look what tables were generated:"""

```py
import duckdb
from google.colab import data_table
data_table.enable_dataframe_formatter()

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()

```

**Question**: How many tables were created?

```Ans: 4```

---


## **Question 3: Explore the loaded data**

Inspect the table `ride`:

```py
df = pipeline.dataset(dataset_type="default").rides.df()
df
```

**Question** :What is the total number of records extracted?

```Ans: 10000```

---

## **Question 4: Trip Duration Analysis**

Run the SQL query below to:

* Calculate the average trip duration in minutes.

```py
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
```

**Quesiton** : What is the average trip duration?

```Ans: 12.3049```



