### 1. Create a schema based on the given dataset
```
create table AgentLogingReport
(
sl_no int,
agent string,
date string,
login string,
logout string,
duration string
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count" = "1");
```
```
create table AgentPerformance
(
sl_no int,
date string,
agent_name string,
total_chats int,
avg_response_time string,
avg_resolution_time string,
avg_rating float,
total_feedback int
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count" = "1");
```
### 2. Dump the data inside the hdfs in the given schema location.
```
load data inpath '/tmp/assignments/' into table AgentPerformance;

load data inpath '/tmp/assignments/' into table AgentLogingReport;
```
The date is put as a string format in the above table is needed to be changed to "date" datatype...
```
create table agentloging
(
sl_no int,
agent string,
date date,
login string,
logout string,
duration string
)
row format delimited
fields terminated by ',';
```
```
insert overwrite table agentloging
select sl_no,
agent,
from_unixtime(unix_timestamp(date,'dd-MMM-yy'),'yyyy-MM-dd'),
login,
logout,
duration
from AgentLogingReport;
```

and same for this table too...
```
create table agentperform
(
sl_no int,
date date,
agent_name string,
total_chats int,
avg_response_time string,
avg_resolution_time string,
avg_rating float,
total_feedback int
)
row format delimited
fields terminated by ',';
```
```
insert overwrite table agentperform
select sl_no,
from_unixtime(unix_timestamp(date,'MM/dd/yyyy'),'yyyy-MM-dd'),
agent_name,
total_chats,
avg_response_time,
avg_resolution_time,
avg_rating,
total_feedback
from AgentPerformance;
```
### 3. List of all agents' names.
```
select distinct agent_name as Names from agentperform;
```
Abhishek<br>
Aditya<br>
Aditya Shinde<br>
Aditya_iot<br>
Amersh<br>
Ameya Jain<br>
Anirudh<br>


### 4. Find out agent average rating.
```
select agent_name, avg(Avg_Rating) as Rating from agentperform group by Agent_name;
```
Abhishek   &nbsp;&nbsp;&nbsp;     0.0<br>
Aditya     &nbsp;&nbsp;&nbsp;    0.0<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp;  1.8003333409627278<br>
Aditya_iot   &nbsp;&nbsp;&nbsp;   2.3453333377838135<br>
Amersh       &nbsp;&nbsp;&nbsp;   0.0<br>
Ameya Jain   &nbsp;&nbsp;&nbsp;   2.21966667175293<br>
Anirudh      &nbsp;&nbsp;&nbsp;   0.6449999968210857<br>

### 5. Total working days for each agents 
```
select agent,count(distinct date) from agentloging group by agent;
```
Aditya Shinde  &nbsp;&nbsp;&nbsp; 1<br>
Aditya_iot   &nbsp;&nbsp;&nbsp;   8<br>
Amersh     &nbsp;&nbsp;&nbsp;     2<br>
Ameya Jain  &nbsp;&nbsp;&nbsp;    7<br>
Ankitjha    &nbsp;&nbsp;&nbsp;    2<br>
Anurag Tiwari &nbsp;&nbsp;&nbsp;  10<br>
Aravind     &nbsp;&nbsp;&nbsp;    7<br>

### 6. Total query that each agent have taken 
```
select agent_name,sum(total_chats) from agentperform group by agent_name;
```
Abhishek    &nbsp;&nbsp;&nbsp;    0<br>
Aditya      &nbsp;&nbsp;&nbsp;    0<br>
Aditya Shinde &nbsp;&nbsp;&nbsp;  277<br>
Aditya_iot   &nbsp;&nbsp;&nbsp;   231<br>
Amersh       &nbsp;&nbsp;&nbsp;   0<br>
Ameya Jain   &nbsp;&nbsp;&nbsp;   322<br>
Anirudh     &nbsp;&nbsp;&nbsp;   81<br>

### 7. Total Feedback that each agent have received 
```
select agent_name,sum(total_feedback) from agentperform group by agent_name;
```
Abhishek    &nbsp;&nbsp;&nbsp;    0<br>
Aditya      &nbsp;&nbsp;&nbsp;    0<br>
Aditya Shinde &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  153<br>
Aditya_iot   &nbsp;&nbsp;&nbsp;   131<br>
Amersh     &nbsp;&nbsp;&nbsp;     0<br>
Ameya Jain  &nbsp;&nbsp;&nbsp;    228<br>
Anirudh    &nbsp;&nbsp;&nbsp;     39<br>

### 8. Agent name who have average rating between 3.5 to 4 
```
select agent_name,avg(avg_rating) as avg_rate from agentperform group by agent_name having avg_rate between 3.5 and 4;
```
Boktiar Ahmed Bappy   &nbsp;&nbsp;&nbsp;  3.567999982833862<br>
Ishawant Kumar   &nbsp;&nbsp;&nbsp;   3.543333347638448<br>
Khushboo Priya   &nbsp;&nbsp;&nbsp;   3.703666663169861<br><br>
Manjunatha A   &nbsp;&nbsp;&nbsp;   3.5946666876475017<br>

### 9. Agent name who have rating less than 3.5 
```
select agent_name,avg(avg_rating) as avg_rate from agentperform group by agent_name having avg_rate < 3.5;
```
Abhishek    &nbsp;&nbsp;&nbsp;    0.0<br>
Aditya      &nbsp;&nbsp;&nbsp;    0.0<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 1.8003333409627278<br>
Aditya_iot   &nbsp;&nbsp;&nbsp;   2.3453333377838135<br>
Amersh       &nbsp;&nbsp;&nbsp;   0.0<br>
Ameya Jain   &nbsp;&nbsp;&nbsp;   2.21966667175293<br>
Anirudh      &nbsp;&nbsp;&nbsp;   0.6449999968210857<br>
Ankit Sharma  &nbsp;&nbsp;&nbsp;  0.0<br>
Ankitjha     &nbsp;&nbsp;&nbsp;   0.26666666666666666<br>

### 10. Agent name who have rating more than 4.5 
```
select agent_name,avg(avg_rating) as avg_rate from agentperform group by agent_name having avg_rate > 4.5;
```
No Output

### 11. How many feedback agents have received more than 4.5 average
```
select count(*) from agentperform group by agent_name having avg(total_feedback) > 4.5;

select agent_name as name, count(total_feedback) as fb from(select distinct(agent_name) as agent_name, total_feedback,avg_rating from agentperform where avg_rating > 4.5) as t group by agent_name;
```
Aditya Shinde &nbsp;&nbsp;&nbsp;  7<br>
Aditya_iot    &nbsp;&nbsp;&nbsp;  5<br>
Ameya Jain    &nbsp;&nbsp;&nbsp;  8<br>
Anirudh       &nbsp;&nbsp;&nbsp;  1<br>
Ankitjha      &nbsp;&nbsp;&nbsp;  1<br>
Aravind       &nbsp;&nbsp;&nbsp;  11<br>
Ayushi Mishra  &nbsp;&nbsp;&nbsp;  7<br>
Bharath       &nbsp;&nbsp;&nbsp;  17<br>
Boktiar Ahmed Bappy  &nbsp;&nbsp;&nbsp;   7<br>

### 12. average weekly response time for each agent.
```
select s.agent_name,avg((t1[0]*3600+t1[1]*60+substr(t1[2],1,2))/60) as timeInminutes ,s.weekly from(select agent_name,split(avg_response_time,':') as t1,weekofyear(Date) as weekly from agentperform)s group by s.agent_name,s.weekly;
```
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  26<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  27<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  28<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  29<br>
Abhishek   &nbsp;&nbsp;&nbsp;     0.0   &nbsp;&nbsp;&nbsp;  30<br>
Aditya     &nbsp;&nbsp;&nbsp;     0.0   &nbsp;&nbsp;&nbsp;  26<br>
Aditya     &nbsp;&nbsp;&nbsp;     0.0   &nbsp;&nbsp;&nbsp;  27<br>
Aditya     &nbsp;&nbsp;&nbsp;     0.0   &nbsp;&nbsp;&nbsp;  28<br>

### 13. average weekly resolution time for each agents 
```
select s.agent_name,avg(((t1[0]*3600+t1[1]*60+substr(t1[2],1,2))/60) as timeInminutes ,s.weekly from(select agent_name,split(avg_resolution_time,':') as t1,weekofyear(Date) as weekly from agentperform)s group by s.agent_name,s.weekly;
```
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  26<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  27<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  28<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  29<br>
Abhishek    &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  30<br>
Aditya      &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  26<br>
Aditya      &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  27<br>
Aditya      &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  28<br>
Aditya      &nbsp;&nbsp;&nbsp;    0.0   &nbsp;&nbsp;&nbsp;  29<br>

### 14. Find the number of chat on which they have received a feedback 
```
select agent_name,sum(total_chats),total_feedback from agentperform where total_feedback > 0 group by agent_name,total_feedback;
```
Aditya Shinde  &nbsp;&nbsp;&nbsp; 8    &nbsp;&nbsp;&nbsp;   7<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 17   &nbsp;&nbsp;&nbsp;   8<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 67   &nbsp;&nbsp;&nbsp;   9<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 18   &nbsp;&nbsp;&nbsp;   11<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 27   &nbsp;&nbsp;&nbsp;   14<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 49   &nbsp;&nbsp;&nbsp;   15<br>
Aditya Shinde  &nbsp;&nbsp;&nbsp; 34   &nbsp;&nbsp;&nbsp;   19<br>

### 15. Total contribution hour for each and every agents weekly basis 
```
select s.agent,sum((t1[0]*3600+t1[1]*60+t1[2])/3600) as timeInHour,s.weekly  from(select agent,split(duration,':') as t1 ,weekofyear(Date) as weekly from agentloging )s group by s.agent,s.weekly limit 7;
```
Aditya Shinde &nbsp;&nbsp;&nbsp;  0.03611111111111111   &nbsp;&nbsp;&nbsp;  30<br>
Aditya_iot    &nbsp;&nbsp;&nbsp;  6.095277777777778     &nbsp;&nbsp;&nbsp;  29<br>
Aditya_iot    &nbsp;&nbsp;&nbsp;  9.635833333333334     &nbsp;&nbsp;&nbsp;  30<br>
Amersh        &nbsp;&nbsp;&nbsp;  3.0638888888888887    &nbsp;&nbsp;&nbsp;  30<br>
Ameya Jain    &nbsp;&nbsp;&nbsp;  24.083055555555553    &nbsp;&nbsp;&nbsp;  29<br>
Ameya Jain    &nbsp;&nbsp;&nbsp;  17.9925               &nbsp;&nbsp;&nbsp;  30<br>
Ankitjha      &nbsp;&nbsp;&nbsp;  2.2669444444444444    &nbsp;&nbsp;&nbsp;  30<br>

### 16. Perform inner join, left join and right join based on the agent column and after joining the table export that data into your local system.

#### Inner Join
```
insert overwrite local directory '/tmp/hive_class/inner_join' 
select a.agent,
a.date,
a.duration,
b.total_charts,
b.total_feedback
from agentloging a join agentperform b
on a.agent = b.agent_name;
```
#### Left Join
```
insert overwrite local directory '/tmp/hive_class/left_join'
select a.agent,
a.date,
a.duration,
b.total_chats,
b.total_feedback
from agentloging a left join agentperform b
on a.agent = b.agent_name;
```
#### Right Join
```
insert overwrite local directory '/tmp/hive_class/right_join' 
select a.agent,
a.date,
a.duration,
b.total_charts,
b.total_feedback 
from agentloging a right join agentperform b
on a.agent = b.agent_name;
```
### 17. Perform partitioning on top of the agent column and then on top of that perform bucketing for each partitioning. 
```
create table agentloggingpart
(
sl_no int,
date date,
login string,
logout string,
duration string
)
partitioned by (agent string)
clustered by (date) sorted by (date) into 4 buckets
row format delimited
fields terminated by ',';
```
```
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
```
```
insert into table agentloggingpart partition(agent)
select sl_no,
date,login,
logout,
duration,
agent
from agentloging;
```
``` 
create table agentperformpart
(
sl_no int,
date date,
total_charts int,
avg_response_time string,
avg_resolution_time string,
avg_rating float,
total_feedback int
)
partitioned by (agent_name string)
clustered by (date) sorted by (date)
into 4 buckets
row format delimited
fields terminated by ',';
```
```
insert into table agentperformpart partition(agent_name) 
select sl_no,date,
total_chats,
avg_response_time,
avg_resolution_time,
avg_rating,
total_feedback,
agent_name
from agentperformance;
``` 


