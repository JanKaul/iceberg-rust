create schema iceberg.test;
create table iceberg.test.customer as select * from tpch.tiny.customer;
create table iceberg.test.lineitem with ( partitioning = ARRAY['month(shipdate)'] ) as select * from tpch.tiny.lineitem;
create table iceberg.test.nation as select * from tpch.tiny.nation;
create table iceberg.test.orders with ( partitioning = ARRAY['month(orderdate)'] ) as select * from tpch.tiny.orders;
create table iceberg.test.part as select * from tpch.tiny.part;
create table iceberg.test.partsupp as select * from tpch.tiny.partsupp;
create table iceberg.test.region as select * from tpch.tiny.region;
create table iceberg.test.supplier as select * from tpch.tiny.supplier;
