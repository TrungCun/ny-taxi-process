COPY_SQL = """
COPY {}
FROM '{}'
ACESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
IGNOREHEADER 1
DELIMITER ';'
"""

COPY_ALL_LOCATIONS_SQL = COPY_SQL.format(
  "staging_locations",
  f's3://ny-taxi-bucket-s3-{{}}/locations/locations_receipts.csv'
)

COPY_SQL_RIDES_SQL = COPY_SQL.format(
  "staging_rides",
  f's3://ny-taxi-bucket-s3-{{}}/rides/rides_receipts.csv'
)

create_staging_locations = ("""
  DROP TABLE IF EXISTS staging_locations;
  CREATE TABLE staging_locations(
    locationID INT IDENTITY(0,1),
    borough VARCHAR(255),
    zone VARCHAR(255),
    service_zone VARCHAR(255)
);
""")

create_staging_rides = ("""
  DROP TABLE IF EXISTS staging_rides;
  CREATE TABLE staging_rides(
    ID SERIAL PRIMARY KEY,
    VendorID INT NOT NULL,
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INT,
    trip_distance DOUBLE NOT NULL,
    RatecodeID INT NOT NULL,
    store_and_fwd_flag VARCHAR(255) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    payment_type INT NOT NULL ,
    fare_amount DOUBLE NOT NULL,
    extra DOUBLE NOT NULL,
    mta_tax DOUBLE NOT NULL,
    tip_amount DOUBLE NOT NULL,
    tolls_amount DOUBLE NOT NULL,
    improvement_surcharge DOUBLE NOT NULL,
    total_amount DOUBLE NOT NULL,
    congestion_surcharge DOUBLE NOT NULL,
    Airport_fee DOUBLE  NOT NULL ,,
);
""")

create_fact_rides= ("""
  DROP TABLE IF EXISTS fact_rides;
  CREATE TABLE fact_rides(
    ID SERIAL PRIMARY KEY,
    VendorID INT NOT NULL,
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INT,
    trip_distance DOUBLE NOT NULL,
    RatecodeID INT NOT NULL,
    store_and_fwd_flag VARCHAR(255) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    payment_type INT NOT NULL,
    fare_amount DOUBLE NOT NULL,
    extra DOUBLE NOT NULL,
    mta_tax DOUBLE NOT NULL,
    tip_amount DOUBLE NOT NULL,
    tolls_amount DOUBLE NOT NULL,
    improvement_surcharge DOUBLE NOT NULL,
    total_amount DOUBLE NOT NULL,
    congestion_surcharge DOUBLE NOT NULL,
    Airport_fee DOUBLE  NOT NULL ,,
);
""")

create_dim_locations = ("""
  DROP TABLE IF EXISTS dim_locations;
  CREATE TABLE dim_locations(
    locationID INT IDENTITY(0,1),
    borough VARCHAR(255),
    zone VARCHAR(255),
    service_zone VARCHAR(255)
);
""")

create_dim_vendor = ("""
  DROP TABLE IF EXISTS dim_vendor;
  CREATE TABLE dim_vendor(
    VendorID INT PRIMARY KEY,
    provider VARCHAR(255) NOT NULL
  );
""")

create_dim_ratecode = ("""
  DROP TABLE IF EXISTS dim_ratecode;
  CREATE TABLE dim_ratecode(
    RatecodeID INT PRIMARY KEY,
    final_rate VARCHAR(255) NOT NULL
  );
""")

create_dim_storefwd = ("""
  DROP TABLE IF EXISTS dim_sotrefwd;
  CREATE TABLE dim_storefwd(
    store_and_fwd_flag VARCHAR(255) PRIMARY KEY,
    store VARCHAR(255) NOT NULL
  );
""")

create_dim_payment = ("""
  DROP TABLE IF EXISTS dim_payment;
  CREATE TABLE dim_payment(
    payment_type INT PRIMARY KEY,
    paid VARCHAR(255) NOT NULL
  );
""")


load_dim_vendor = ("""
  INSERT INTO dim_vendor values (1, 'Creative Mobile Technologies'),
  (2, 'VeriFone Inc');
""")

load_dim_ratecode  = ("""
  INSERT INTO dim_ratecode values (1, 'Standard rate'),
  (2, 'JFK')
  (3, 'Newark')
  (4, 'Nassau or Westchester')
  (5, 'Negotiated fare')
  (6, 'Group ride');
""")

load_dim_storefwd  = ("""
  INSERT INTO dim_storefwd values ('Y', 'store'),
  ('N', 'not a store');
""")

load_dim_payment  = ("""
  INSERT INTO dim_payment values (1, 'Credit card'),
  (2, 'Cash')
  (3, 'No charge')
  (4, 'Dispute')
  (5, 'Unknown')
  (6, 'Voided trip');
""")

load_dim_locations = ("""
  INSERT INTO dim_locations (locationID, borough, zone, service_zone)
  SELECT locationID, borough, zone, service_zone
  FROM staging_locations;
""")

load_fact_rides = ("""
  INSERT INTO fact_rides (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee)

  SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee

  FROM staging_rides;
""")


drop_staging = ("""
  DROP TABLE IF EXISTS staging_rides;
  DROP TABLE IF EXISTS staging_locations;
""")

