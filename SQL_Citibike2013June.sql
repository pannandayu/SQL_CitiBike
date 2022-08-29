-- Trip Count
select distinct date(starttime) as date, count(starttime) over(partition by date(starttime)) as trip_count
from citibike_csv cc 
order by 2 desc;

-- Average Cycling Time (per date)
with time
as 
(
	select date(starttime) as date,
		   cast(starttime as time) as start_time,
		   cast(stoptime as time) as stop_time,
		   timediff(cast(starttime as time), cast(stoptime as time)) as time_diff
	from citibike_csv cc
)
select distinct date, concat(round(avg(abs(time_to_sec(time_diff))) over(partition by date)/60,2), ' ', 'minutes') as avg_time
from time
order by 2 desc;

-- Checking Trip Volume and Cycling Duration Correlation
select n.date, n.trip_count, at2.avg_time
from 
(select distinct date(starttime) as date, count(starttime) over(partition by date(starttime)) as trip_count
from citibike_csv cc 
order by 2 desc) as n
join avg_time at2 
on n.date = at2.date
order by 2 desc, 3;

-- Generation Count
with gen 
as 
(
	select *,
	case 
		when birth_year between 1922 and 1927 then 'WW2'
		when birth_year between 1928 and 1945 then 'Post War'
		when birth_year between 1946 and 1954 then 'Boomers 1'
		when birth_year between 1955 and 1964 then 'Boomers 2'
		when birth_year between 1965 and 1980 then 'Gen X'
		when birth_year between 1981 and 1996 then 'Millenials'
		when birth_year between 1977 and 2012 then 'Gen Z'
		else 'Unkown'
	end as generation
	from citibike_csv cc
)
select distinct generation, count(generation) over(partition by generation) as gen_count
from gen
order by 2 desc;

-- Average Cycling Time Each Generation
with gen 
as 
(
	select *,
	case 
		when birth_year between 1922 and 1927 then 'WW2'
		when birth_year between 1928 and 1945 then 'Post War'
		when birth_year between 1946 and 1954 then 'Boomers 1'
		when birth_year between 1955 and 1964 then 'Boomers 2'
		when birth_year between 1965 and 1980 then 'Gen X'
		when birth_year between 1981 and 1996 then 'Millenials'
		when birth_year between 1977 and 2012 then 'Gen Z'
		else 'Unkown'
	end as generation
	from citibike_csv cc
)
select generation, concat(round(avg(tripduration)/60,2), ' ', 'minutes') as avg_minutes
from gen
group by 1
order by 2 desc;

-- Top 15 Starting Station for Annual Member
select distinct start_station_name, count(start_station_id) over(partition by start_station_name) as trip_count
from citibike_csv cc 
where usertype = 'Subscriber'
order by 2 desc
limit 15;

-- Gender Count
with gender 
as 
(
	select *,
	case 
		when gender = 1 then 'Male'
		when gender = 2 then 'Female'
		else 'Unkown'
	end as gender_type
	from citibike_csv cc
)
select distinct gender_type, count(gender_type) over(partition by gender_type) as gender_count
from gender
order by 1;

-- Cycling Growth
select date, trip_count, diff, perc_change
from
(
	with growth
	as
	(
		select distinct date(starttime) as date, count(starttime) over(partition by date(starttime)) as trip_count
		from citibike_csv cc
		order by 1 limit 30
	)
	select *, lag(trip_count,1) over(order by date) as lagging,
		  trip_count - lag(trip_count,1) over(order by date) as diff,
		  round((trip_count - lag(trip_count,1) over(order by date))/lag(trip_count,1) over(order by date)*100,2) as perc_change
	from growth
) as growth

-- Top 10 Destination (Start and Stop Station)
select start_station_name, end_station_name, count(*) as trip_count
from citibike_csv cc 
group by 1,2 
order by 3 desc limit 10;

-- Average Trip Duration by Specific Destination
with duration
as
(
	select start_station_name, end_station_name,
	cast(starttime as time) as start_time,
	cast(stoptime as time) as stop_time,
	abs(time_to_sec(timediff(cast(starttime as time),cast(stoptime as time)))) as time_diff
	from citibike_csv cc
)
select start_station_name, end_station_name, round(avg(time_diff)/60,2) as avg_time_minutes
from duration
group by 1,2;

-- Top 10 Used Bikes
select distinct bikeid, count(*) over(partition by bikeid order by bikeid) as num_trips
from citibike_csv cc
order by 2 desc limit 10;