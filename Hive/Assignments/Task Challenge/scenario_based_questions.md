#### Q1. Will the reducer work or not if you use “Limit 1” in any HiveQL query?
```
  YES IT WORKS SAME FOR LIMIT 1 AS FOR ANY OTHER HIVEQL QUERY
```

#### Q2. Suppose I have installed Apache Hive on top of my Hadoop cluster using default metastore configuration. 
Then, what will happen if we have multiple clients trying to access Hive at the same time? 
```
  THERE WON'T BE SUPPORT FOR MULTIPLE CONNECTIONS FOR DEFAULT METASTORE I.E. DERBY RDBMS AS IT DOESN'T ALLOW MORE THAN ONE CONNECTION AT A TIME
```

#### Q3. Suppose, I create a table that contains details of all the transactions done by the customers: 
CREATE TABLE transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;
Now, after inserting 50,000 records in this table, I want to know the total revenue generated for each month. But, Hive is taking too much time in processing this query. 
How will you solve this problem and list the steps that I will be taking in order to do so?
```
  PARTITION BASED QN

		create table tran_cust
		        (
			cust_id int,
			amount float,
			)
			partitioned by (month string);

		loading data from main table to partitioned table:

		insert overwrite tran_cust(month) select cust_id, amount,amount, month from transaction_details;
```
#### Q4. How can you add a new partition for the month December in the above partitioned table?
```
 ALTER TABLE tran_cust ADD PARTITION (month='DECEMBER') location '/tran_cust/month = December';  
```
#### Q5. I am inserting data into a table based on partitions dynamically. But, I received an error – 
FAILED ERROR IN SEMANTIC ANALYSIS: Dynamic partition strict mode requires at least one static partition column. How will you remove this error?
```
By using 'set hive.exec.dynamic.partition.mode=nonstrict;' we remove the strict nature of hive over the table and hence allows the dynamic partition to iterate over the table and create necessary partitions.
```

#### Q6. Suppose, I have a CSV file – ‘sample.csv’ present in ‘/temp’ directory with the following entries:
id first_name last_name email gender ip_address
How will you consume this CSV file into the Hive warehouse using built-in SerDe?
```
  create table csv_table
	(
		ID INT,
		FIRST_NAME STRING,
		LAST_NAME STRING,
		EMAIL STRING,
		GENDER STRING,
		IP_ADDRESS STRING
	)
	row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
	STORED AS TEXTFILE
	TBLPROPERTIES("SKIP.HEADER.LINE.COUNT" = "1");

	load data from local:

	load data local inpath 'file:///tmp/hive_class/sample.csv' into table csv_table;

```

#### Q7. Suppose, I have a lot of small CSV files present in the input directory in HDFS and I want to create a single Hive table corresponding to these files. 
The data in these files are in the format: {id, name, e-mail, country}. Now, as we know, Hadoop performance degrades when we use lots of small files.
So, how will you solve this problem where we want to create a single Hive table for lots of small files without degrading the performance of the system?
```
  WE CAN STORE ALL THOSE SMALL FILES IN A SEQUENCE, INSIDE A DEDICATED FOLDER HOLDING THESE TABLE RELATED TO SAME SCHEMA AND CAN LOAD DATA USING-
	LOAD DATA LOCAL INPATH AND INSTEAD OF DIRECTING TO SINGLE FILE WE DIRECT IT TO THE FOLDER HOLDING THE SMALL TABLES
 ```

#### Q8. LOAD DATA LOCAL INPATH ‘Home/country/state/’OVERWRITE INTO TABLE address;


The following statement failed to execute. What can be the cause?
```
  1. The data set file format may be diffrent from that of the hive schema
	2. The directory may be empty i.e. no data set file.
 
 ```

#### Q9. Is it possible to add 100 nodes when we already have 100 nodes in Hive? If yes, how?
```
  YES, by putting the the whole setup to safe mode and admin will connect more nodes to the existing system.

```
-----------------------------
### Hive Practical questions:
Hive Join operations

Create a  table named CUSTOMERS(ID | NAME | AGE | ADDRESS   | SALARY)
Create a Second  table ORDER(OID | DATE | CUSTOMER_ID | AMOUNT
)

Now perform different joins operations on top of these tables
(Inner JOIN, LEFT OUTER JOIN ,RIGHT OUTER JOIN ,FULL OUTER JOIN)

Inner JOIN:
```
select * from customers c INNER  JOIN ORDER o ON (c.ID = o.customer_id);

c.id    c.name &nbsp; c.age    c.address     c.salary     o.oid   o.date  o.customer_id   o.amount<br>
1      sam     20    berlin      2000.0  11      20/08/2021      1      20.0
2       tim     22      austria      1000.0  10    20/12/2020      2       12.0
3       dan     20      rome          2500.0  12     12/02/2021      3       15.0
4       lao    23      japan       3000.0  12      30/10/2021     4       18.0
5       ram    21     india        1500.0  10    03/05/2021      5       12.0

```

LEFT OUTER JOIN:

	 select * from customers c LEFT OUTER JOIN ORDER o ON (c.ID = o.customer_id);

	c.id    c.name  c.age   c.address       c.salary        o.oid   o.date  o.customer_id   o.amount
	1       sam     20      berlin  	2000.0  11      20/08/2021      1       20.0
	2       tim     22      austria 	1000.0  10      20/12/2020      2       12.0
	3       dan     20      rome    	2500.0  12      12/02/2021      3       15.0
	4       lao     23      japan   	3000.0  12      30/10/2021      4       18.0
	5       ram     21      india   	1500.0  10      03/05/2021      5       12.0


RIGHT OUTER JOIN:

	select * from customers c RIGHT OUTER JOIN ORDER o ON (c.ID = o.customer_id);

	c.id    c.name  c.age   c.address       c.salary        o.oid   o.date  o.customer_id   o.amount
	2       tim     22      austria 1000.0  10      20/12/2020      2       12.0
	3       dan     20      rome    2500.0  12      12/02/2021      3       15.0
	5       ram     21      india   1500.0  10      03/05/2021      5       12.0
	1       sam     20      berlin  2000.0  11      20/08/2021      1       20.0
	4       lao     23      japan   3000.0  12      30/10/2021      4       18.0


FULL OUTER JOIN:

	select * from customers c FULL OUTER JOIN ORDER o ON (c.ID = o.customer_id);

	c.id    c.name  c.age   c.address       c.salary        o.oid   o.date  o.customer_id   o.amount
	1       sam     20      berlin  	2000.0  	11      20/08/2021      1       20.0
	2       tim     22      austria 	1000.0  	10      20/12/2020      2       12.0
	3       dan     20      rome    	2500.0  	12      12/02/2021      3       15.0
	4       lao     23      japan   	3000.0  	12      30/10/2021      4       18.0
	5       ram     21      india   	1500.0  	10      03/05/2021      5       12.0

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### BUILD A DATA PIPELINE WITH HIVE

##### Download a data from the given location - 
##### https://archive.ics.uci.edu/ml/machine-learning-databases/00360/ <br>

#### 1. Create a hive table as per given schema in your dataset 
```
	create table airqualityuci
        (
	date string, 
	time string, 
	co_gt float, 
	pt08_s1_co int,
	nmhc_gt int,
	c6h6_gt float,
	pt08_s2_nmhc int,
	nox_gt int,
	pt08_s3_nox int,
	no2_gt int,
	pt08_s4_no2 int,
	pt08_s5_o3 int,
	t float,
	rh float,
	ah float
	) 
	row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	with serdeproperties
	"separatorChar"= "\;",
	"escapeChar"= "\\"
	)
	stored as textfile
	tblproperties("skip.header.line.count" = "1");

```
#### 2. try to place a data into table location
```
	load data local inpath 'file:///tmp/hive_class/AirQualityUCI121.csv' into table airqualityuci;
```
#### 3. Perform a select operation . 
```
select * from airqualityuci limit 2;

uci.date      uci.time       uci.co_gt      uci.pt08_s1_co   uci.nmhc_gt    uci.c6h6_gt   uci.pt08_s2_nmhc  uci.nox_gt    uci.pt08_s3_  nox uci.no2_gt   uci.pt08_s4_no2 uci.pt08_s5_o3     uci.t   uci.rh uci.ah
10/03/2004      18.00.00        2.6    1360    150     11.9   1046    166    1056    113     1692    1268    13.6   48.9   0.7578
10/03/2004    19.00.00       2       1292    112     9.4     955     103    1174    92     1559   972     13.3    47.7  &nbsp;  0.7255
```


#### 4. Fetch the result of the select operation in your local as a csv file . 
```


```
	

#### 5. Perform group by operation . 

```
	select date, time, t, co_gt  from airqualityuci group by date,time,t,co_gt limit 10;

	uci.date  	uci.time    uci.t      uci.co_gt
	01/01/2005     00.00.00       8.2    -200
	01/01/2005     01.00.00        5.3     1.6
	01/01/2005      02.00.00        5.9     2.5
	01/01/2005     03.00.00       4.9     2.7
	01/01/2005     04.00.00        4.3     1.9
	01/01/2005      05.00.00       4.2     1.4
	01/01/2005      06.00.00       3.5    1.5
	01/01/2005      07.00.00        3.0     1.4
	01/01/2005     08.00.00       2.6    1.1


```

#### 7. Perform filter operation at least 5 kinds of filter examples . 
	
```
	select date,time,t,co_gt from airqualityuci where date = '19/03/2004' group by date,time,t,co_gt limit 10;


	uci.date	uci.time	uci.t		uci.co_gt	
	19/03/2004      00.00.00        12.0    2
	19/03/2004      01.00.00        11.9    1.6
	19/03/2004      02.00.00        12.5    0.9
	19/03/2004      03.00.00        12.5    0.7
	19/03/2004      04.00.00        12.3    -200
	19/03/2004      05.00.00        12.5    0.5
	19/03/2004      06.00.00        12.3    0.7
	19/03/2004      07.00.00        12.4    1.5
	19/03/2004      08.00.00        13.0    4.8
	19/03/2004      09.00.00        13.6    6.2

```

#### 8. show and example of regex operation

```
select * from airqualityuci where ah regexp '^0.66';

uci.date      uci.time      uci.co_gt    uci.pt08_s1_co  uci.nmhc_gt    uci.c6h6_gt   uci.pt08_s2_nmhc      uci.nox_gt     uci.pt08_s3_  nox uci.no2_gt     uci.pt08_s4_no2 uci.pt08_s5_o3     uci.t  uci.rh  uci.ah
11-03-2004      23:00:00        1.4    913     26      2.6     629     47      1565    53  1252     552     8.2     60.8    0.6657
12-03-2004      15:00:00        2.3     1353    185     14.2    1122    190     922     126 1740     1139    15.8    37.0    0.661
12-03-2004      16:00:00        2.7     1309    165     12.7    1073    178     954     120 1657     1112    15.9    37.2    0.6657
22-03-2004      15:00:00        2.1     1094    -200    10.7    1003    130     965     99  1574     813     21.3    26.8    0.6681
22-03-2004      17:00:00        2.7     1152    185     12.4    1062    138     928     103 1606     850     20.2    28.5    0.6682

```

#### 9. alter table operation 
```

alter table airqualityuci change date day string;
	
	uci.day		uci.time 	uci.t 	uci.co_gt
	01/01/2005      00.00.00        8.2     -200
  
```

#### 10 . drop table operation
```
create table air_quali_bkup_csv as select * from airqualityuci;

drop table air_quali_bkup_csv;
  
```

#### 12 . order by operation. 

```
	
select day, time from airqualityuci group by day,time order by time limit 10;
	
	day     time

	16/06/2004      00.00.00
	31/12/2004      00.00.00
	31/10/2004      00.00.00
	31/08/2004      00.00.00
	31/07/2004      00.00.00
	31/05/2004      00.00.00
	31/03/2005      00.00.00
	31/03/2004      00.00.00
	31/01/2005      00.00.00
	
  
```

#### 13 . where clause operations you have to perform.

```
	
select date,time,t,co_gt from airqualityuci where date = '19/03/2004' group by date,time,t,co_gt limit 10;


	uci.date	uci.time	uci.t		uci.co_gt	
	19/03/2004      00.00.00        12.0    2
	19/03/2004      01.00.00        11.9    1.6
	19/03/2004      02.00.00        12.5    0.9
	19/03/2004      03.00.00        12.5    0.7
	19/03/2004      04.00.00        12.3    -200
	19/03/2004      05.00.00        12.5    0.5
	19/03/2004      06.00.00        12.3    0.7
	19/03/2004      07.00.00        12.4    1.5
	19/03/2004      08.00.00        13.0    4.8
	19/03/2004      09.00.00        13.6    6.2
  
```

#### 14 . sorting operation you have to perform. 

```
select day, time, co_gt from airqualityuci sort by day, time;
uci.day 	uci.time	uci.co_gt
31/03/2005      16.00.00        1.1
31/03/2005      17.00.00        1.5
31/03/2005      18.00.00        2.1
31/03/2005      19.00.00        2.1
31/03/2005      20.00.00        1.5
31/03/2005      21.00.00        1.0
31/03/2005      22.00.00        0.8
31/03/2005      23.00.00        0.8
31/05/2004      00.00.00        0.2
31/05/2004      01.00.00        0.3
31/05/2004      02.00.00        -200
31/05/2004      03.00.00        -200
31/05/2004      04.00.00        -200
31/05/2004      05.00.00        -200
31/05/2004      06.00.00        0.5
31/05/2004      07.00.00        1.6
31/05/2004      08.00.00        3.6	

```

#### 15 . distinct operation you have to perform. 

```
select distinct(day) from airqualityuci;

       	01/01/2005
	01/02/2005
	01/03/2005
	01/04/2004
	01/04/2005
	01/05/2004
	01/06/2004
	01/07/2004
	01/08/2004
	01/09/2004
	01/10/2004
	01/11/2004
	01/12/2004
	02/01/2005
	02/02/2005
	
```
  

#### 16 . like an operation you have to perform . 
		
```
select distinct(day) from airqualityuci where day like '%2004';

	01/04/2004
	01/05/2004
	01/06/2004
	01/07/2004
	01/08/2004
	01/09/2004
	01/10/2004
	01/11/2004
	01/12/2004
	02/04/2004
	02/05/2004
	02/06/2004
	02/07/2004
```

#### 17 . union operation you have to perform. 

```
select distinct(time), c6h6 from airqualityuci where time = '10:00:00'  limit 10 union all select distinct(time), c6h6 from airqualityuci where time = '11:00:00'  limit 10;

time           c6h6
10:00:00        2.8
10:00:00        2.7
10:00:00        2.6
10:00:00        2.5
10:00:00        2.3
10:00:00        2.0
10:00:00        1.9
10:00:00        1.4
10:00:00        0.5
10:00:00        -200.0
11:00:00        3.2
11:00:00        3.1
11:00:00        3.0
11:00:00        2.7
11:00:00        2.6
11:00:00        2.5
11:00:00        2.3
11:00:00        2.0
11:00:00        0.6
```


#### 18 . table view operation you have to perform. 

```

```







### HIVE OPERATION WITH PYTHON

Create a python application that connects to the Hive database for extracting data, 
creating sub tables for data processing, drops temporary tables.fetch rows to python itself into a list of tuples and mimic the join or filter operations
