SELECT pickup_area, dropdown_area ,APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90
FROM
(select pickup.zone AS pickup_area, dropdown.zone AS dropdown_area, TIMESTAMP_DIFF(trip.dropOff_datetime, trip.pickup_datetime, SECOND)AS trip_duration,
        FORMAT_DATE('%m', DATE(trip.pickup_datetime)) AS month,
        FORMAT_DATE('%Y', DATE(trip.pickup_datetime)) AS year
FROM ny_taxi.FHV_tripdata AS trip
RIGHT JOIN ny_taxi.dim_zone AS pickup ON pickup.locationid = trip.PUlocationID
RIGHT JOIN ny_taxi.dim_zone AS dropdown ON dropdown.locationid = trip.DOlocationID
WHERE pickup_datetime >= TIMESTAMP('2019-11-01') AND pickup_datetime < TIMESTAMP('2019-12-01') 
AND pickup.zone in ('Newark Airport', 'SoHo',  'Yorkville East')
)
GROUP BY year, month, pickup_area, dropdown_area 
ORDER BY pickup_area, p90 DESC