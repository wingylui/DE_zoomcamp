SELECT service_type, year, month,
       APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97,
       APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95,
       APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90
FROM (
  SELECT service_type, fare_amount,
         FORMAT_DATE('%m', DATE(pickup_datetime)) AS month,
         FORMAT_DATE('%Y', DATE(pickup_datetime)) AS year
  FROM ny_taxi.fact_trips
  WHERE fare_amount > 0 
    AND trip_distance > 0 
    AND LOWER(payment_type_description) in ('cash', 'credit card')
    AND pickup_datetime >= TIMESTAMP('2020-04-01') 
    AND pickup_datetime < TIMESTAMP('2020-05-01')
) AS subquery
GROUP BY service_type, year, month
ORDER BY service_type, year, month;

