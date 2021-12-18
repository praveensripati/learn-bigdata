
create external table ontime (
  Year INT,
  Month INT,
  DayofMonth INT,
  DayOfWeek INT,
  DepTime  INT,
  CRSDepTime INT,
  ArrTime INT,
  CRSArrTime INT,
  UniqueCarrier STRING,
  FlightNum INT,
  TailNum STRING,
  ActualElapsedTime INT,
  CRSElapsedTime INT,
  AirTime INT,
  ArrDelay INT,
  DepDelay INT,
  Origin STRING,
  Dest STRING,
  Distance INT,
  TaxiIn INT,
  TaxiOut INT,
  Cancelled INT,
  CancellationCode STRING,
  Diverted STRING,
  CarrierDelay INT,
  WeatherDelay INT,
  NASDelay INT,
  SecurityDelay INT,
  LateAircraftDelay INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://airlinedata-praveen/input/';

CREATE TABLE ontime_parquet_snappy
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://airline-dataset-praveen/athena-export-to-parquet'
    ) AS SELECT * FROM ontime;

select Origin, count(*) from ontime where DepTime > CRSDepTime group by Origin;

select Origin, count(*) from ontime_parquet_snappy where DepTime > CRSDepTime group by Origin;