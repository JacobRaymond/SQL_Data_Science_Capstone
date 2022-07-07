-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Milestone 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/athlete_events-4.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "TRUE"
-- MAGIC first_row_is_header = "TRUE"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC temp_table_name = "olympics"
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

/* Check import */
select * from olympics

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/olympic_clean", recurse=True)

-- COMMAND ----------

DROP TABLE IF EXISTS olympic_clean;

CREATE TABLE if not exists olympic_clean
  AS (
    SELECT ID, Name, NOC, Year, Sport, Medal, Event
    FROM olympics 
    where Games in ("1988 Winter", 
      "1984 Winter", "1980 Winter", "1976 Winter", "1972 Winter",
      "1992 Winter", "1994 Winter", "1998 Winter", "2002 Winter")
  ) ;
  
  cache table olympic_clean

-- COMMAND ----------

select * from olympic_clean

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/noc_regions-2.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC temp_table_name = "noc"
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

select * from noc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/olympic_final", recurse=True)

-- COMMAND ----------

DROP TABLE IF EXISTS olympic_final;

/* added line to recode ex-Soviet countries */
CREATE TABLE if not exists olympic_final
  AS (
    SELECT ID, Name, olympic_clean.NOC, Year, Sport, Event,
    (CASE WHEN medal = 'Gold' THEN 1 ELSE 0 END) as Gold,
    (CASE WHEN medal = 'Silver' THEN 1 ELSE 0 END) as Silver,
    (CASE WHEN medal = 'Bronze' THEN 1 ELSE 0 END) as Bronze,
    (case when noc.region in ("Russia", "Estonia", "Latvia", "Lithuania", "Belarus", "Ukraine", "Moldova", "Georgia", "Armenia", "Azerbaijan", "Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan", "Tajikistan") then "USSR/Ex-Soviet" else noc.region end ) as Region
    FROM olympic_clean
    left join noc on olympic_clean.NOC=noc.NOC
  );

cache table olympic_final

-- COMMAND ----------

select * from olympic_final

-- COMMAND ----------

/* Descriptive stats */
CREATE VIEW if not exists TotMed AS
select Year, region, Sport, Event, Gold+Silver+Bronze as TotMed
from olympic_final;

select * from TotMed

-- COMMAND ----------

/* Total Medals by Country */


create view if not exists TotMedTot as
select region, sum(TotMed) as Total_Medals from TotMed where TotMed>0 group by region order by Total_Medals desc;

select * from TotMedTot;

-- COMMAND ----------

/* Total Medals by Country by Year (only top 10 countries)*/
select region, Year, sum(TotMed) as Total_Medals from TotMed 
where region in (select region from TotMedTot limit 10) 
group by region, Year 
order by region, Year;

-- COMMAND ----------

/* Athlete Count Change, 1972, 1988, 2002*/
Select region, sum(ath72) as Count_72,
sum(ath88) as Count_88,
sum(ath02) as Count_02,
sum(ath02)/(sum(ath72)+1) as Change7202
from
(Select Distinct * from (Select ID, region, 
(CASE WHEN year = 1972 THEN 1 ELSE 0 END) as ath72,
(CASE WHEN year = 1988 THEN 1 ELSE 0 END) as ath88,
(CASE WHEN year = 2002 THEN 1 ELSE 0 END) as ath02
from olympic_final))
group by region
order by Change7202 desc

-- COMMAND ----------

/* Dominating country by Sport  */
select Sport, region, sum(G72) as Gold72, sum(G88) as Gold88,sum(G02) as Gold02
from (Select Sport, region, Year, 
(CASE WHEN year = 1972 and Gold=1 THEN 1 ELSE 0 END) as G72,
(CASE WHEN year = 1988 and Gold=1  THEN 1 ELSE 0 END) as G88,
(CASE WHEN year = 2002 and Gold=1 THEN 1 ELSE 0 END) as G02
from olympic_final)
group by Sport, region
Having sum(G72)>0 or sum(G88)>0 or Gold02>0
order by Sport, Gold02 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Milestone 3

-- COMMAND ----------

/* Correlation between medals and athlete counts */
select corr(Total_Medals, Ath_Count) from 
(select region, Year, sum(Gold+Silver+Bronze) as Total_Medals, count(distinct ID) as Ath_Count from olympic_final group by region, year)

-- COMMAND ----------

/* Athlete Count and Medal Count per Year (Asian Countries) */
select corr(Year, Total_Medals), corr(Year, Ath_Count), corr(Total_Medals, Ath_Count) from (
select Year, sum(Gold+Silver+Bronze) as Total_Medals, count(distinct ID) as Ath_Count from olympic_final  where region in ("China", "Japan", "South Korea", "Malaysia", "Taiwan") group by year order by year)



-- COMMAND ----------

/* Identify Team Events*/
drop view if exists Team_Sports;

create view Team_Sports as
select distinct Event from (select Sport, Event, sum(Gold), Year from olympic_final group by Sport, Event, Year having sum(Gold)>1);

select * from Team_Sports

/* After research, Cross Country Skiing Men's 10/10 kilometres Pursuit should not be included; there were two gold medals in 2002 due to a technicality*/ 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/team_sports_cnt", recurse=True)

-- COMMAND ----------

/*Approximate number of athletes per team */
drop table if exists Team_Sports_Cnt;
create table if not exists Team_Sports_Cnt (Event varchar(200), Ath_Cnt_Den int);

insert into Team_Sports_Cnt(Event, Ath_Cnt_Den)
values
("Figure Skating Mixed Ice Dancing", 2),
("Cross Country Skiing Men's 4 x 10 kilometres Relay", 4),
("Bobsleigh Men's Four", 4),
("Figure Skating Mixed Pairs", 2),
("Bobsleigh Women's Two", 2),
("Biathlon Women's 4 x 7.5 kilometres Relay", 4),
("Biathlon Men's 4 x 7.5 kilometres Relay", 4),
("Luge Mixed (Men)'s Doubles", 2),
("Ski Jumping Men's Large Hill, Team", 4),
("Nordic Combined Men's Team", 3.5),
("Cross Country Skiing Women's 4 x 5 kilometres Relay", 4),
("Bobsleigh Men's Two", 2),
("Cross Country Skiing Women's 3 x 5 kilometres Relay", 3),
("Biathlon Women's 3 x 7.5 kilometres Relay", 3),
("Curling Men's Curling", 5),
("Curling Women's Curling", 5), 
("Short Track Speed Skating Men's 5,000 metres Relay", 5),
("Short Track Speed Skating Women's 3,000 metres Relay", 4),
("Ice Hockey Men's Ice Hockey", 22),
("Ice Hockey Men's Ice Hockey", 21)
;

select * from Team_Sports_Cnt

-- COMMAND ----------

drop view if exists event_den;

create view event_den as
select distinct olympic_final.Event, ifNULL(Team_Sports_Cnt.Ath_Cnt_Den, 1) as den from olympic_final
left join Team_Sports_Cnt on olympic_final.Event=Team_Sports_Cnt.Event;

select * from event_den

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The actual analysis
-- MAGIC 
-- MAGIC Will consist in: <br/>
-- MAGIC 1. Count the number of medals for each sport for three periods: 1972-1984 (average), 1988, 1992-2002 (average), grouped by country. Divide by the number of athletes/teams, and see which countries had the highest growth overall.
-- MAGIC 2. Repeat this analysis at the aggregate region level
-- MAGIC 3. Repeat 1 and 2 with focus on USSR/ex. Soviet (hypothesis)
-- MAGIC 4. Repeat 1 and 2 with focus on Asia (hypothesis)
-- MAGIC 5. Countries with greatest growth in athletes (pre-1988 avg, 1988, post-1988 avg)
-- MAGIC 6. Regression: overall, for each year, is there a link between the number of athletes/teams and the number of medals?

-- COMMAND ----------

/* Medals table (accounts for team sports) */
drop view if exists pre1988;
drop view if exists post1988;
drop view if exists yyc;
drop view if exists medals;
drop view if exists Ath;

-- COMMAND ----------

create view pre1988 as
select region, Sport, Event, sum(Gold+Silver+Bronze) as Medals_pre1988, count(ID) as Ath_Count_pre1988
from olympic_final 
where year<1988
group by region, Sport, Event
order by region;

select * from pre1988

-- COMMAND ----------

create view yyc as
select region, Sport, Event, sum(Gold+Silver+Bronze) as Medals_1988, count(ID) as Ath_Count_1988
from olympic_final 
where year=1988
group by region, Sport, Event
order by region;

select * from yyc

-- COMMAND ----------

create view post1988 as
select region, Sport, Event, sum(Gold+Silver+Bronze) as Medals_post1988, count(ID) as Ath_Count_post1988
from olympic_final 
where year>1988
group by region, Sport, Event
order by region; 

select * from post1988

-- COMMAND ----------

/* Medals */
create view medals as
select region, Sport, Event, Medals_pre1988/(4*Ath_Count_pre1988/den) as Pre1988_Medal_Avg, Medals_1988/(Ath_Count_1988/den) as 1988_Medals, Medals_post1988/(4*Ath_Count_Post1988/den) as Post1988_Medal_Avg from (
select df2.region, df2.Sport, df2.Event, df2.Medals_pre1988, df2.Medals_1988, df2.Medals_post1988, df2.Ath_Count_pre1988,  df2.Ath_Count_1988,  df2.Ath_Count_post1988, event_den.den from
  (select df1.region, df1.Sport, df1.Event, df1.Medals_pre1988, df1.Ath_Count_pre1988, df1.Ath_Count_1988, df1.Medals_1988, post1988.Medals_post1988, post1988.Ath_Count_post1988 from
    (select yyc.region, yyc.Sport, yyc.Event, yyc.Ath_Count_1988,  pre1988.Medals_pre1988, pre1988.Ath_Count_pre1988, yyc.Medals_1988
    from yyc
    join pre1988 on yyc.region=pre1988.region and yyc.Event=pre1988.Event) df1
    join post1988 on df1.region=post1988.region and df1.Event=post1988.Event) df2
join event_den on df2.Event=event_den.Event
);


-- COMMAND ----------

select * from medals limit 10

-- COMMAND ----------

/* Medal Growth (Question 1) */
select region, sum(1988_Medals)+sum(Pre1988_Medal_Avg) as PreMed, sum(Post1988_Medal_Avg) as PostMed, sum(Post1988_Medal_Avg)-(0.2*sum(1988_Medals)+0.8*sum(Pre1988_Medal_Avg)) as Medal_Growth  from Medals
group by region
order by Medal_Growth asc

-- COMMAND ----------

/* Medal Growth (Question 2) */
select region, sport, sum(1988_Medals)+sum(Pre1988_Medal_Avg) as PreMed, sum(Post1988_Medal_Avg) as PostMed, sum(Post1988_Medal_Avg)-(0.2*sum(1988_Medals)+0.8*sum(Pre1988_Medal_Avg)) as Medal_Growth  from Medals
group by region, sport
order by Medal_Growth asc

-- COMMAND ----------

/* USSR Performance (Q3) */
select region, sum((Post1988_Medal_Avg/((0.2*1988_Medals+0.8*Pre1988_Medal_Avg)+0.0001))-1) as Medal_Growth  from Medals
where region="USSR/Ex-Soviet"
group by region
order by Medal_Growth desc

-- COMMAND ----------

select region, sport, sum((Post1988_Medal_Avg/((0.2*1988_Medals+0.8*Pre1988_Medal_Avg)+0.0001))-1) as Medal_Growth  from Medals
where region="USSR/Ex-Soviet"
group by region, sport
order by Medal_Growth desc

-- COMMAND ----------

/* Asian Country Performance (Q3) */
select region, sum((Post1988_Medal_Avg/((0.2*1988_Medals+0.8*Pre1988_Medal_Avg)+0.0001))-1) as Medal_Growth  from Medals
where region in ("China", "Japan", "South Korea", "Malaysia", "Taiwan")
group by region
order by Medal_Growth desc

-- COMMAND ----------

select region, sport, sum((Post1988_Medal_Avg/((0.2*1988_Medals+0.8*Pre1988_Medal_Avg)+0.0001))-1) as Medal_Growth  from Medals
where region in ("China", "Japan", "South Korea", "Malaysia", "Taiwan")
group by region, sport
order by Medal_Growth desc

-- COMMAND ----------

/* Athlete Count (Q5) */
create view Ath as
select region, Sport, Event, Ath_Count_pre1988/4 as Pre1988_Ath_Avg, Ath_Count_1988 as Ath_1988, Ath_Count_Post1988/4 as Post1988_Ath_Avg from (
select df2.region, df2.Sport, df2.Event, df2.Medals_pre1988, df2.Medals_1988, df2.Medals_post1988, df2.Ath_Count_pre1988,  df2.Ath_Count_1988,  df2.Ath_Count_post1988, event_den.den from
  (select df1.region, df1.Sport, df1.Event, df1.Medals_pre1988, df1.Ath_Count_pre1988, df1.Ath_Count_1988, df1.Medals_1988, post1988.Medals_post1988, post1988.Ath_Count_post1988 from
    (select yyc.region, yyc.Sport, yyc.Event, yyc.Ath_Count_1988,  pre1988.Medals_pre1988, pre1988.Ath_Count_pre1988, yyc.Medals_1988
    from yyc
    join pre1988 on yyc.region=pre1988.region and yyc.Event=pre1988.Event) df1
    join post1988 on df1.region=post1988.region and df1.Event=post1988.Event) df2
join event_den on df2.Event=event_den.Event
);

select * from Ath

-- COMMAND ----------

select region, sum(Post1988_Ath_Avg/((0.2*Ath_1988+0.8*Pre1988_Ath_Avg)+0.1)) as Ath_Growth from Ath
group by region
order by Ath_Growth desc

-- COMMAND ----------

drop view if exists reg

-- COMMAND ----------

/* Regression (Q6) */
create view reg as
Select region, Sport, Event, log(Medals_pre1988) as Meds, log(Ath_Count_pre1988) as Ath from
(Select * from pre1988
union all 
Select * from yyc
union all 
Select * from post1988)

-- COMMAND ----------

select * from reg

-- COMMAND ----------

Select sum(Meds-Avg_Meds)/sum(Ath-Avg_Ath) as Slope from (
Select * from reg join (Select Avg(Meds) as Avg_Meds, Avg(Ath) as Avg_Ath from reg) df) df1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## For module 4

-- COMMAND ----------

select df2.region, Sport, sum(df2.Medals_pre1988)+sum(df2.Medals_1988), sum(df2.Medals_post1988) from
  (select df1.region, df1.Sport, df1.Event, df1.Medals_pre1988, df1.Ath_Count_pre1988, df1.Ath_Count_1988, df1.Medals_1988, post1988.Medals_post1988, post1988.Ath_Count_post1988 from
    (select yyc.region, yyc.Sport, yyc.Event, yyc.Ath_Count_1988,  pre1988.Medals_pre1988, pre1988.Ath_Count_pre1988, yyc.Medals_1988
    from yyc
    join pre1988 on yyc.region=pre1988.region and yyc.Event=pre1988.Event) df1
    join post1988 on df1.region=post1988.region and df1.Event=post1988.Event) df2
join event_den on df2.Event=event_den.Event
group by region, Sport
having region="USA"
