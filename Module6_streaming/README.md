# Module 6 : Stream Processing
---
Instead of Kafka, we will use Red Panda, which is a drop-in replacement for Kafka. It implements the same interface, so we can use the Kafka library for Python for communicating with it, as well as use the Kafka connector in PyFlink.

For this homework we will be using the Taxi data:

Green 2019-10 data from here

## Setup: 
---
1. Run ```docker compose up``` in docker file
2. Access Flink Job Manager: [localhost:8081](http://localhost:8081)
3. Access pgAdmin: [localhost:8080](http://localhost:8080)
4. Connecting crednetial 
```
Username : postgres
Password : postgres
Database : postgres
Host     : localhost
Port     : 5432
```
5. Running new query
```
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);
```
---
### Question 1: Redpanda version

What's the version, based on the output of the command you executed? (copy the entire version)

1. Run command ```docker exec redpanda-1 rpk --version``` to get the redpanda version

```Ans: v24.2.18 ```
---

### Question 2. Creating a topic
Before we can send data to the redpanda server, we need to create a topic. We do it also with the rpk command we used previously for figuring out the version of redpandas.

Read the output of help and based on it, create a topic with name green-trips

1. create green-trip topic in redpanda ```docker exec redpanda-1 rpk topic create greenTaxi-10-2019-topic```

What's the output of the command for creating a topic? Include the entire output in your answer.

```
Ans:
TOPIC                    STATUS
greenTaxi-10-2019-topic  OK
```

---

### Question 3. Connecting to the Kafka server
We need to make sure we can connect to the server, so later we can send some data to its topics

To connect to the server, run ```kafka_connection.ipynb```

Provided that you can connect to the server, what's the output of the last command?

```Ans: True```

---

### Question 4: Sending the Trip Data
Now we need to send the data to the green-trips topic

Read the data, and keep only these columns:
```
'lpep_pickup_datetime',
'lpep_dropoff_datetime',
'PULocationID',
'DOLocationID',
'passenger_count',
'trip_distance',
'tip_amount'
```
How much time did it take to send the entire dataset and flush?

```Ans: 82.79738593101501```

---

### Question 5: Build a Sessionization Window (2 points)

```
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py --pyFiles /opt/src/job -d
```


Now we have the data in the Kafka stream. It's time to process it.

Copy aggregation_job.py and rename it to session_job.py
Have it read from green-trips fixing the schema
Use a session window with a gap of 5 minutes
Use lpep_dropoff_datetime time as your watermark with a 5 second tolerance
Which pickup and drop off locations have the longest unbroken streak of taxi trips?