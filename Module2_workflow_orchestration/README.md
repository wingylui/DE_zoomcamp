# Module 2: Workflow Orchestration

### Dataset for this assignment
- Green new york taxi (Jan 2019 - July 2020)
- Yellow new york taxi (Jan 2019 - July 2020)

### Setting up Kestra

```
docker run --pull=always --rm -it \
    -p 8080:8080 \
    --user=root \
    -v //var/run/docker.sock:/var/run/docker.sock \
    -v /tmp:/tmp kestra/kestra:latest server local
```

### Loading data into Google Cloud Perform
```
1. Setup GCP by using GCP.yaml
2. Ingesting datasets on to GCP using data_load.yaml 
3. Using backfill to automatically ingest multiple csv files
```

---

### Quiz Questions

1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?<br>
```Ans : 128.3 MB```


2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?<br>
{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv


```Ans: green_tripdata_2020-04.csv```



3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

```Ans: 24,648,499```


4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?

```Ans: 1,734,051```


5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?<br>
```Ans: 1,925,152```


How would you configure the timezone to New York in a Schedule trigger?

```Ans: Add a timezone property set to America/New_York in the Schedule trigger configuration```
