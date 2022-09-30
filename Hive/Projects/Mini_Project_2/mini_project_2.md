```
create table parking_violations
(
Summons_Number bigint,
Plate_ID string,
Registration_State string,
Plate_Type string,
Issue_Date string,
Violation_Code int,
Vehicle_Body_Type string,
Vehicle_Make string,
Issuing_Agency string,
Street_Code1 int,
Street_Code2 int,
Street_Code3 int,
Vehicle_Expiration string,
Violation_Location int,
Violation_Precinct int,
Issuer_Precinct int,
Issuer_Code int,
Issuer_Command string,
Issuer_Squad string,
Violation_Time string,
Time_First_Observed string,
Violation_County string,
Violation_In_Front_Of_Or_Opposite string,
House_Number string,
Street_Name string,
Intersecting_Street string,
Date_First_Observed int,
Law_Section int,
Sub_Division string,
Violation_Legal_Code string,
Days_Parking_In_Effect string,
From_Hours_In_Effect string,
To_Hours_In_Effect string,
Vehicle_Color string,
Unregistered_Vehicle int,
Vehicle_Year string,
Meter_Number string,
Feet_From_Curb int,
Violation_Post_Code string,
Violation_Description string,
No_Standing_or_Stopping_Violation string,
Hydrant_Violation string,
Double_Parking_Violation string)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count" = "1");
```
```
create table violations_parking
(
Summons_Number bigint,
Plate_ID string,
Registration_State string,
Plate_Type string,
Issue_Date date,
Violation_Code int,
Vehicle_Body_Type string,
Vehicle_Make string,
Issuing_Agency string,
Street_Code1 int,
Street_Code2 int,
Street_Code3 int,
Vehicle_Expiration date,
Violation_Location int,
Violation_Precinct int,
Issuer_Precinct int,
Issuer_Code int,
Issuer_Command string,
Issuer_Squad string,
Violation_Time string,
Time_First_Observed string,
Violation_County string,
Violation_In_Front_Of_Or_Opposite string,
House_Number string,
Street_Name string,
Intersecting_Street string,
Date_First_Observed int,
Law_Section int,
Sub_Division string,
Violation_Legal_Code string,
Days_Parking_In_Effect string,
From_Hours_In_Effect string,
To_Hours_In_Effect string,
Vehicle_Color string,
Unregistered_Vehicle int,
Vehicle_Year string,
Meter_Number string,
Feet_From_Curb int,
Violation_Post_Code string,
Violation_Description string,
No_Standing_or_Stopping_Violation string,
Hydrant_Violation string,
Double_Parking_Violation string)
row format delimited
fields terminated by ',';
```
 ```
 insert overwrite table violations_parking select
 Summons_Number bigint,
 Plate_ID string,
 Registration_State string,
 Plate_Type string,
 from_unixtime(unix_timestamp(Issue_Date,'MM/dd/YYYY'),'yyyy-MM-dd'),
 Violation_Code int,
 Vehicle_Body_Type string,
 Vehicle_Make string,
 Issuing_Agency string,
 Street_Code1 int,
 Street_Code2 int,
 Street_Code3 int,
 from_unixtime(unix_timestamp(Vehicle_Expiration,'YYYYMMdd'),'yyyy-MM-dd'),
 Violation_Location int,
 Violation_Precinct int,
 Issuer_Precinct int,
 Issuer_Code int,
 Issuer_Command string,
 Issuer_Squad string,
 Violation_Time string,
 Time_First_Observed string,
 Violation_County string,
 Violation_In_Front_Of_Or_Opposite string,
 House_Number string,
 Street_Name string,
 Intersecting_Street string,
 Date_First_Observed int,
 Law_Section int,
 Sub_Division string,
 Violation_Legal_Code string,
 Days_Parking_In_Effect string,
 From_Hours_In_Effect string,
 To_Hours_In_Effect string,
 Vehicle_Color string,
 Unregistered_Vehicle int,
 Vehicle_Year string,
 Meter_Number string,
 Feet_From_Curb int,
 Violation_Post_Code string,
 Violation_Description string,
 No_Standing_or_Stopping_Violation string,
 Hydrant_Violation string,
 Double_Parking_Violation string
 from parking_violations;
 ```
``` 
create table park_viol_part_buck
(
Summons_Number bigint,
Plate_ID string,
Registration_State string,
Plate_Type string,
Issue_Date date,
Violation_Code int,
Vehicle_Body_Type string,
Vehicle_Make string,
Issuing_Agency string,
Street_Code1 int,
Street_Code2 int,
Street_Code3 int,
Vehicle_Expiration date,
Violation_Location int,
Violation_Precinct int,
Issuer_Precinct int,
Issuer_Code int,
Issuer_Command string,
Issuer_Squad string,
Violation_Time string,
Time_First_Observed string,
Violation_In_Front_Of_Or_Opposite string,
House_Number string,
Street_Name string,
Intersecting_Street string,
Date_First_Observed int,
Law_Section int,
Sub_Division string,
Violation_Legal_Code string,
Days_Parking_In_Effect string,
From_Hours_In_Effect string,
To_Hours_In_Effect string,
Vehicle_Color string,
Unregistered_Vehicle int,
Vehicle_Year string,
Meter_Number string,
Feet_From_Curb int,
Violation_Post_Code string,
Violation_Description string,
No_Standing_or_Stopping_Violation string,
Hydrant_Violation string,
Double_Parking_Violation string)
partitioned by (Violation_County string)
clustered by (Violation_Code)
sorted by(Violation_Code) into 8 buckets
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count" = "1");
```
```
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.enforce.bucketing = true;
```
```
insert into park_viol_part_buck partition(Violation_County) select
Summons_Number,Plate_ID,Registration_State,Plate_Type,Issue_Date,Violation_Code,
Vehicle_Body_Type,Vehicle_Make,Issuing_Agency,Street_Code1,Street_Code2,
Street_Code3,Vehicle_Expiration,Violation_Location,Violation_Precinct,
Issuer_Precinct,Issuer_Code,Issuer_Command,Issuer_Squad,Violation_Time,
Time_First_Observed,Violation_In_Front_Of_Or_Opposite,House_Number,Street_Name,
Intersecting_Street,Date_First_Observed,Law_Section,Sub_Division,Violation_Legal_Code,
Days_Parking_In_Effect,From_Hours_In_Effect,To_Hours_In_Effect,Vehicle_Color,
Unregistered_Vehicle,Vehicle_Year,Meter_Number,Feet_From_Curb,Violation_Post_Code,
Violation_Description,No_Standing_or_Stopping_Violation,Hydrant_Violation,
Double_Parking_Violation,Violation_County from violations_parking
where year(Issue_Date) = '2017';
```

select count(distinct Summons_Number) Tickets_Total ,year(Issue_Date) as year from park_viol_part_buck group by year(Issue_Date);
 
select count(distinct Registration_State) as No_of_States from park_viol_part_buck;
select Registration_State, Count(1) as Number_of_Records from park_viol_part_buck  group by Registration_State order by Number_of_Records;


select count(distinct summons_number) as No_Tickets_without_address from violations_parking  where Street_code1 = 0 or Street_code2 = 0 or Street_code3 = 0;





select count(Violation_Code) as frequency_of_violation,Violation_Code from park_viol_part_buck group by Violation_Code order by frequency_of_violation desc limit 5;

select Vehicle_Body_Type,count(summons_number)as frequency_of_getting_parking_ticket  from park_viol_part_buck group by Vehicle_Body_Type order by frequency_of_getting_parking_ticket desc limit 5;
select Vehicle_make,count(summons_number)as frequency_of_getting_parking_ticket from park_viol_part_buck group by Vehicle_make order by frequency_of_getting_parking_ticket desc limit 5;

a.
select Violation_Precinct,count(*) as IssuedTicket from violations_parking group by  Violation_Precinct order by IssuedTicket desc limit 6;

b.
select Issuer_Precinct,count(*) as IssuedTicket from violations_parking group by Issuer_Precinct order by IssuedTicket desc limit 6;

select Issuer_Precinct,Violation_Code, count(*) as TicketsIssued from park_viol_part_buck  group by Issuer_Precinct, Violation_Code order by TicketsIssued desc limit 7;
\
select Violation_Code, count(*) as TicketsIssued from park_viol_part_buck where Issuer_Precinct=18 group by Violation_Code order by TicketsIssued desc limit 7;
/
select Violation_Code, count(*) as TicketsIssued from park_viol_part_buck where Issuer_Precinct=19 group by Violation_Code order by TicketsIssued desc limit 7;
\
select Violation_Code, count(*) as TicketsIssued from park_viol_part_buck where Issuer_Precinct=14 group by Violation_Code order by TicketsIssued desc limit 7;
/
select Issuer_Precinct,Violation_Code, count(*) as TicketsIssued from park_viol_part_buck where Issuer_Precinct in (18,19,14) group by Issuer_Precinct,Violation_Code order by TicketsIssued desc limit 10;


select from_unixtime(unix_timestamp(regexp_extract(violation_time,'(.*)[A-Z]',1),'HHmm'),"HH:mm") as date_data from violations_parking limit 2;
/
select from_unixtime(unix_timestamp(concat(violation_time,'M'), 'HHmmaaa'),"HH:mmaaa") as date_data from violations_parking limit 2;



create view park_viol_part_view partitioned on (Violation_Code) as
select Summons_Number, Violation_Time, Issuer_Precinct,
case
when substring(Violation_Time,1,2) in ('00','01','02','03','12') and upper(substring(Violation_Time,-1))='A' then 1
when substring(Violation_Time,1,2) in ('04','05','06','07') and upper(substring(Violation_Time,-1))='A' then 2
when substring(Violation_Time,1,2) in ('08','09','10','11') and upper(substring(Violation_Time,-1))='A' then 3
when substring(Violation_Time,1,2) in ('12','00','01','02','03') and upper(substring(Violation_Time,-1))='P' then 4
when substring(Violation_Time,1,2) in ('04','05','06','07') and upper(substring(Violation_Time,-1))='P' then 5
when substring(Violation_Time,1,2) in ('08','09','10','11') and upper(substring(Violation_Time,-1))='P'then 6
else null end as Violation_Time_bin,Violation_Code
from park_viol_part_buck
where Violation_Time is not null or (length(Violation_Time)=5 and upper(substring(Violation_Time,-1))in ('A','P')
and substring(Violation_Time,1,2) in ('00','01','02','03','04','05','06','07', '08','09','10','11','12'));


select Violation_Code,count(*) TicketsIssued from park_viol_part_view where Violation_Time_bin == 1 group by Violation_Code order by TicketsIssued desc limit 3;
select Violation_Code,count(*) TicketsIssued from park_viol_part_view where Violation_Time_bin == 2 group by Violation_Code order by TicketsIssued desc limit 3;
select Violation_Code,count(*) TicketsIssued from park_viol_part_view where Violation_Time_bin == 3 group by Violation_Code order by TicketsIssued desc limit 3;
select Violation_Code,count(*) TicketsIssued from park_viol_part_view where Violation_Time_bin == 4 group by Violation_Code order by TicketsIssued desc limit 3;
select Violation_Code,count(*) TicketsIssued from park_viol_part_view where Violation_Time_bin == 5 group by Violation_Code order by TicketsIssued desc limit 3;
select Violation_Code,count(*) TicketsIssued from park_viol_part_view where Violation_Time_bin == 6 group by Violation_Code order by TicketsIssued desc limit 3;


select Violation_Time_bin, count(*) TicketsIssued from park_viol_part_view where Violation_Code in (21,36,37,38) group by Violation_Time_bin order by TicketsIssued desc limit 3;



create view tickets_issued_view as
select Violation_Code , Issuer_Precinct,
case
when MONTH(Issue_Date) between 03 and 05 then 'spring'
when MONTH(Issue_Date) between 06 and 08 then 'summer'
when MONTH(Issue_Date) between 09 and 11 then 'autumn'
when MONTH(Issue_Date) in (1,2,12) then 'winter'
else 'unknown' end  as season 
from violations_parking;
	
	
create view tickets_issued_view_part partitioned on (Violation_Code) as
select Issuer_Precinct,
case
when MONTH(Issue_Date) between 03 and 05 then 'spring'
when MONTH(Issue_Date) between 06 and 08 then 'summer'
when MONTH(Issue_Date) between 09 and 11 then 'autumn'
when MONTH(Issue_Date) in (1,2,12) then 'winter'
else 'unknown' end  as season,Violation_Code from
violations_parking;
	
	
select season, count(*) as TicketsIssued from tickets_issued_view_part group by season order by TicketsIssued desc;

select Violation_Code, count(*) as TicketsIssued from tickets_issued_view_part where season = 'spring' group by Violation_Code order by TicketsIssued desc limit 6;

select Violation_Code, count(*) as TicketsIssued from tickets_issued_view_part where season = 'summer' group by Violation_Code order by TicketsIssued desc limit 6;

select Violation_Code, count(*) as TicketsIssued from tickets_issued_view_part where season = 'autumn' group by Violation_Code order by TicketsIssued desc limit 6;

select Violation_Code, count(*) as TicketsIssued from tickets_issued_view_part where season = 'winter' group by Violation_Code order by TicketsIssued desc limit 6;
