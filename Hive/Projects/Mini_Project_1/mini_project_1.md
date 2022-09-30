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
	
load data inpath '/tmp/assignments/' into table AgentLogingReport;
	
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


insert overwrite table agentloging select sl_no,agent,from_unixtime(unix_timestamp(date,'dd-MMM-yy'),'yyyy-MM-dd'),login,logout,duration from AgentLogingReport;

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
	
load data inpath '/tmp/assignments/' into table AgentPerformance;

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

insert overwrite table agentperform select sl_no,from_unixtime(unix_timestamp(date,'MM/dd/yyyy'),'yyyy-MM-dd'),agent_name,total_chats,avg_response_time,avg_resolution_time,avg_rating,total_feedback from AgentPerformance;


select * from agentlogingreport limit 5;
select * from agentperformance limit 5;


select distinct agent_name as Names from agentperform limit 7;

select agent_name, avg(Avg_Rating) as Rating from agentperform group by Agent_name limit 7;

select agent,count(distinct date) from agentloging group by agent limit 7;

select agent_name,sum(total_chats) from agentperform group by agent_name limit 7;

select agent_name,sum(total_feedback) from agentperform group by agent_name limit 7;

select agent_name,avg(avg_rating) as avg_rate from agentperform group by agent_name having avg_rate between 3.5 and 4;

select agent_name,avg(avg_rating) as avg_rate from agentperform group by agent_name having avg_rate < 3.5;

select agent_name,avg(avg_rating) as avg_rate from agentperform group by agent_name having avg_rate > 4.5;

select count(*) from agentperform group by agent_name having avg(total_feedback) > 4.5;
select agent_name as name, count(total_feedback) as fb from(select distinct(agent_name) as agent_name, total_feedback,avg_rating from agentperform where avg_rating > 4.5) as t group by agent_name;

select s.agent_name,avg(col1[0]*3600+col1[1]*60+substr(col1[2],1,2))/60 as timeInminutes ,s.weekly from(select agent_name,split(avg_response_time,':') as col1,weekofyear(Date) as weekly from agentperform)s group by s.agent_name,s.weekly;

select s.agent_name,avg((col1[0]*3600+col1[1]*60+substr(col1[2],1,2))/60 as timeInminutes ,s.weekly from(select agent_name,split(avg_resolution_time,':') as col1,weekofyear(Date) as weekly from agentperform)s group by s.agent_name,s.weekly limit 7;

select agent_name,sum(total_chats),total_feedback from agentperform where total_feedback > 0 group by agent_name,total_feedback limit 7;

select s.agent,sum((col1[0]*3600+col1[1]*60+col1[2])/3600) as timeInHour,s.weekly  from(select agent,split(duration,':') as col1 ,weekofyear(Date) as weekly from agentloging )s group by s.agent,s.weekly limit 7;



 insert overwrite local directory '/tmp/hive_class/inner_join' select a.agent,a.date,a.duration,b.total_charts,b.total_feedback from agentloging a join agentperform b on a.agent = b.agent_name;
 
 insert overwrite local directory '/tmp/hive_class/left_join' select a.agent,a.date,a.duration,b.total_chats,b.total_feedback from agentloging a left join agentperform b on a.agent = b.agent_name;
 
 insert overwrite local directory '/tmp/hive_class/right_join' select a.agent,a.date,a.duration,b.total_charts,b.total_feedback from agentloging a right join agentperform b on a.agent = b.agent_name;
 
 
 
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


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 

insert into table agentloggingpart partition(agent) select sl_no,date,login,logout,duration,agent from agentloging;
 
 
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

insert into table agentperformpart partition(agent_name) select sl_no,date,total_chats,avg_response_time,avg_resolution_time,avg_rating,total_feedback,agent_name from agentperformance;
 


