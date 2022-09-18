# HIVE ASSIGNMENT
## BRIEF DESCRIPTION

<B>This assignment was performed to develop and check skills for HDFS and Hive like:</b>
    <ol>
     <li>copying/moving files from lfs to hdfs</li>
      <li>creating table in hive</li>
     <li>loading data to a ORC format table</li>
      <li>performing basic queries</li>
  </ol>


### LOADING DATA FROM HOST SYSTEM TO LFS AND THEN TO HDFS

Used File Zilla to transfer file into lfs

hadoop fs -mkdir /tmp/assignment<br>
hadoop fs -put sales_order_data.csv /tmp/assignment

### CREATING A CSV TABLE WITH SCHEMA AND DATATYPE RELATED TO THE ABOVE DATASET

```
create table sales_order_csv
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1")
; 
```

### LOADING DATA FROM HDFS TO sales_order_csv</b>

load data inpath '/tmp/assignment/' into table sales_order_csv;

### CREATING AN ORC TABLE WITH SCHEMA SIMILAR TO CSV

```
create table sales_order_orc
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;
```

### LOADING DATA FROM sales_order_csv TO sales_order_orc
from sales_order_csv insert overwrite table sales_order_orc select *;

### QUERIES
  
a. Calculate total sales per year:
```
select year_id, sum(sales) as total_sales from sales_order_orc group by year_id;
```
year_id &nbsp; total_sales<br>
2003  &nbsp;  3516979.547241211<br>
2004   &nbsp; 4724162.593383789<br>
2005    &nbsp; 1791486.7086791992<br>



b. Find a product for which maximum orders were placed:
```
select sum(quantityordered) as total_orders, productline from sales_order_orc group by productline order by total_orders desc limit 1;
```
total_orders &nbsp; productline<br>
33992 &nbsp; Classic Cars<br>

c. Calculate the total sales for each quarter:
```
select qtr_id, sum(sales) as total_sales from sales_order_orc group by qtr_id;
```
qtr_id   &nbsp;  total_sales
1    &nbsp;   2350817.726501465<br>
2    &nbsp;   2048120.3029174805<br>
3    &nbsp;   1758910.808959961<br>
4    &nbsp;    3874780.010925293<br>

d. In which quarter sales was minimum:
```
select qtr_id, sum(sales) as total_sales from sales_order_orc group by qtr_id order by total_sales asc limit 1;
```
qtr_id &nbsp; total_sales<br>
3     &nbsp;  1758910.808959961<br>

e. In which country sales was maximum and in which country sales was minimum:
```
select country, sum(sales) as total_sales from sales_order_orc group by country order by total_sales asc limit 1 union all select country, sum(sales) as total_sales from sales_order_orc group by country order by total_sales desc limit 1;
```
_u1.country  &nbsp;   _u1.total_sales
Ireland &nbsp; 57756.43029785156 >>>>>>> Minimum Sales<br>
USA    &nbsp;  3627982.825744629 >>>>>>> Maximum Sales<br>


f. Calculate quartelry sales for each city:
```
select city, qtr_id, sum(sales) as total_sales from sales_order_orc group by city,qtr_id limit 10;
```
city   &nbsp; qtr_id &nbsp; total_sales<br>
Aaarhus &nbsp; 4   &nbsp;    100595.5498046875<br>
Allentown    &nbsp;   2   &nbsp;    6166.7998046875<br>
Allentown    &nbsp;   3   &nbsp;    71930.61041259766<br>
Allentown    &nbsp;   4    &nbsp;   44040.729736328125<br>
Barcelona    &nbsp;   2    &nbsp;   4219.2001953125<br>
Barcelona    &nbsp;   4   &nbsp;    74192.66003417969<br>
Bergamo     &nbsp;   1    &nbsp;   56181.320068359375<br>
Bergamo     &nbsp;     4    &nbsp;   81774.40008544922<br>
Bergen      &nbsp;    3    &nbsp;   16363.099975585938<br>
Bergen      &nbsp;    4     &nbsp;  95277.17993164062<br>


g. Find a month for each year in which maximum number of quantities were sold:
```
select year_id,month_id,total_sales from (select year_id, month_id,total_sales,dense_rank() over (partition by year_id order by total_sales desc) as level from(select year_id,month_id,sum(sales) as total_sales from sales_order_orc group by year_id,month_id)t1)t2 where level =1;
```
year_id &nbsp; month_id     &nbsp;   total_sales<br>
2003   &nbsp;  11   &nbsp;   1029837.6627197266<br>
2004   &nbsp;  11   &nbsp;   1089048.0076293945<br>
2005  &nbsp;  5    &nbsp;   457861.06036376953<br>
