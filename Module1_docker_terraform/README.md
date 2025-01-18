# Module 1: Containerization and Infrastructure as Code (Homework)

### Question 1. Understanding docker first run
Run docker with the python:3.12.8 image in an interactive mode, use the entrypoint bash.

<b>Method:</b>
1. Install python 3.12.8 image in docker
    - ```docker run -it python:3.12.8```
2. Exit the python container by using ```Ctrl + D ```
3. Run entrypoint bash to use <b>pip</b>
    - ```docker run -it --entrypoint=bash python:3.12.8```
4. Extract the version of pip in the image
    - ```pip -V```


<b> What's the version of pip in the image?</b> </br>
``` Answer: 24.3.1```

----

### Question 2. Understanding Docker networking and docker-compose
```
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```
Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?

``` Answer: postgres:5432```

The diagram below explained the connection of pgAdmin and postgres
![pgadminExplain](./docker_postgres/Postgres%20Container.png) 


## Prepare dataset inside docker containers
1. Connecting postgres using pgAdmin
```docker network create postgres-pg-connect```

2. Create postgres container
```
docker run -it \
  -e POSTGRES_USER="postgres"\
  -e POSTGRES_PASSWORD="password"\
  -e POSTGRES_DB="ny_taxi"\
  -p 5433:5432\
  -v "{ file path }"\
  --network=postgres-pg-connect\
  --name db_postgres\
  postgres:17-alpine
```
3. Log into the database </br>
```pgcli -h localhost -p 5433 -u postgres -d ny_taxi```

4. Loading datasets into postgres

5. Create pgAdmin 4
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="pgadmin@pgadmin.com"\
  -e PGADMIN_DEFAULT_PASSWORD="pgadmin" \
  -p 8080:80 \
  --network=postgres-pg-connect\
  --name pgadmin\
  dpage/pgadmin4:latest
```

6. Create a new server group in pgAdmin to connect postgres database
```
host name: db_postgres
port: 5432
```
---

### Question 3. Trip Segmentation Count
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened: </br>

- Up to 1 mile
- In between 1 (exclusive) and 3 miles (inclusive),
- In between 3 (exclusive) and 7 miles (inclusive),
- In between 7 (exclusive) and 10 miles (inclusive),
- Over 10 miles

```Answers:104,802; 198,924; 109,603; 27,678; 35,189```
</br>

<b>SQL Query</b>
```
SELECT distance_group, COUNT(distance_group) AS Trip_count
FROM
	(SELECT 
		CASE 
			WHEN trip_distance <= 1 THEN 'up to 1 miles'
			WHEN trip_distance BETWEEN 1 AND 3 THEN '1 to 3 miles'
			WHEN trip_distance BETWEEN 3 AND 7 THEN '3 to 7 miles'
			WHEN trip_distance BETWEEN 7 AND 10 THEN '7 to 10 miles'
			ELSE 'over 10 miles'
		END AS distance_group
	FROM public.green_taxi_trip_2019_10
	WHERE lpep_pickup_datetime >= DATE'2019-10-01' AND lpep_dropoff_datetime < DATE'2019-11-01')
GROUP BY distance_group;
```

---
### Question 4. Longest trip for each day
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

```Answer: 2019-10-31```

<b>SQL query</b>
```
SELECT lpep_pickup_datetime, trip_distance
FROM public.green_taxi_trip_2019_10
WHERE trip_distance = (SELECT MAX(trip_distance) FROM public.green_taxi_trip_2019_10);
```


---
### Question 5. Three biggest pickup zones
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?


```Answer: East Harlem North, East Harlem South, Morningside Heights```

<b>SQL query:</b>
```
SELECT SUM(t.total_amount) AS total_amount, z."Zone"
FROM public.green_taxi_trip_2019_10 AS t
INNER JOIN public.taxi_zone AS z ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime >= DATE'2019-10-18' 
	AND t.lpep_pickup_datetime < DATE'2019-10-19'
GROUP BY z."Zone"
ORDER BY total_amount DESC;
```
---
### Question 6. Largest tip
For the passengers picked up in October 2019 in the zone name "East Harlem North" which was the drop off zone that had the largest tip?

```Answer: JFK Airport```
</br>

<b>SQL query:</b>
```
SELECT d."Zone" AS drop_off_zone, t.tip_amount AS largest_tip
FROM public.green_taxi_trip_2019_10 AS t
INNER JOIN public.taxi_zone AS d ON t."DOLocationID" = d."LocationID"
WHERE t.tip_amount = (
						SELECT MAX(tip_amount) AS largest_tip
						FROM public.green_taxi_trip_2019_10 AS t
						INNER JOIN public.taxi_zone AS p ON t."PULocationID" = p."LocationID"
						WHERE t.lpep_pickup_datetime >= DATE'2019-10-01' 
							AND t.lpep_pickup_datetime < DATE'2019-11-01'
							AND p."Zone" = 'East Harlem North'
						GROUP BY p."Zone"
						)
```
---

## Terraform


### Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:

- Downloading the provider plugins and setting up backend
- Generating proposed changes and auto-executing the plan
- Remove all resources managed by terraform`

```Answers: terraform init, terraform apply -auto-approve, terraform destroy```

