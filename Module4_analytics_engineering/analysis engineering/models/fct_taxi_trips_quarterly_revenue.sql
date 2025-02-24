SELECT * , 
  CASE WHEN Year = '2019' THEN null 
  ELSE (total_amount - lastRow)/total_amount * 100 
  END
FROM
(
    SELECT *, lag(total_amount) over (order by service_type, Quarter, Year) AS lastRow
    FROM
    (
        SELECT Quarter, Year, service_type, SUM(total_amount) AS total_amount,

        FROM
        (
        SELECT format_date('%Q', date(pickup_datetime)) AS Quarter ,
            format_date('%Y', date(pickup_datetime)) AS Year,
            service_type, total_amount ,pickup_datetime
        FROM ny_taxi.fact_trips
        WHERE pickup_datetime >= timestamp('2019-01-01') and pickup_datetime <= timestamp('2021-01-01')
        )

        GROUP BY Quarter, Year, service_type
        -- ORDER BY service_type, Quarter, Year ASC
    )
)
ORDER BY service_type, Quarter, Year ASC