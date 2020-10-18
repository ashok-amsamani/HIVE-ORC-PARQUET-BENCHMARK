create table staging_txn(txnno INT, txndate STRING, custno INT, amount DOUBLE, category varchar(20), product varchar(20), city varchar(20), state varchar(20), spendby varchar(20)) 
row format delimited fields terminated by ',' lines terminated by '\n' 
stored as textfile;

LOAD DATA LOCAL INPATH '/home/hduser/hive/data/txns' OVERWRITE INTO TABLE staging_txn; 

select count(*) from staging_txn
--tot records in staging_txn: 95904


create table txn_parquet(txnno INT, txndate STRING, custno INT, amount DOUBLE,category varchar(20), product varchar(20), city varchar(20), state varchar(20), spendby varchar(20)) 
row format delimited fields terminated by ',' lines terminated by '\n' stored as parquetfile; 

Insert into table txn_parquet select txnno,txndate,custno,amount,category, product,city,state,spendby from staging_txn;
--MapReduce Total cumulative CPU time: 2 seconds 790 msec
--Time taken to load from staging table: 10.565 seconds


select count(*) from txn_parquet
--tot records in txn_parquet: 95904


create table txn_orc(txnno INT, txndate STRING, custno INT, amount DOUBLE, category varchar(20), product varchar(20), city varchar(20), state varchar(20), spendby varchar(20)) 
row format delimited fields terminated by ',' lines terminated by '\n' stored as orcfile; 

Insert into table txn_orc select txnno,txndate,custno,amount,category, product,city,state,spendby from staging_txn; 
--MapReduce Total cumulative CPU time: 1 seconds 960 msec
--Time taken to load from staging table:  8.566 seconds


select count(*) from txn_orc
--tot records in txn_orc: 95904


--Time Taken to load data into HDFS: ORC < Parquest (Since ORC compression is more, it transfers data quickly)
--Space Occupied in HDFS: ORC < Parquet (ORC has good compression)


select count(txnno),category from staging_txn group by category; 
--Time taken to retrieve data from HDFS: 11.604 seconds, Fetched: 15 row(s)  => no decompression required for txtfile, hence faster.

select count(txnno),category from txn_parquet group by category;
--Time taken to retrieve data from HDFS: 12.529 seconds, Fetched: 15 row(s)

select count(txnno),category from txn_orc group by category; 
--Time taken to retrieve data from HDFS: 12.531 seconds, Fetched: 15 row(s) => ORC has more compression. So it takes more time than parquet.

