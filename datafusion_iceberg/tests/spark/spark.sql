CREATE SCHEMA demo.test;

CREATE TABLE demo.test.customer (
    custkey BIGINT,
    name STRING,
    nationkey BIGINT
) USING ICEBERG;

INSERT INTO demo.test.customer VALUES
  (1, 'Alice', 1),
  (2, 'Bob', 2),
  (3, 'Charlie', 3);

CREATE TABLE demo.test.orders (
    orderkey BIGINT,
    custkey BIGINT,
    totalprice DOUBLE,
    orderdate DATE
) USING ICEBERG
PARTITIONED BY (months(orderdate));

INSERT INTO demo.test.orders VALUES
  (1, 1, 100.50, DATE '2020-01-15'),
  (2, 1, 200.25, DATE '2020-01-20'),
  (3, 2, 150.75, DATE '2020-02-10'),
  (4, 3, 300.00, DATE '2020-02-15'),
  (5, 1, 50.50, DATE '2020-03-01');

CREATE TABLE demo.test.lineitem (
    orderkey BIGINT,
    partkey BIGINT,
    quantity DOUBLE,
    extendedprice DOUBLE,
    shipdate DATE
) USING ICEBERG
PARTITIONED BY (months(shipdate));

INSERT INTO demo.test.lineitem VALUES
  (1, 100, 10.0, 50.0, DATE '2020-01-20'),
  (1, 101, 5.0, 30.0, DATE '2020-01-25'),
  (2, 100, 20.0, 100.0, DATE '2020-02-01'),
  (3, 102, 15.0, 75.0, DATE '2020-02-15'),
  (4, 103, 30.0, 200.0, DATE '2020-03-10'),
  (5, 100, 8.0, 25.0, DATE '2020-03-15');
