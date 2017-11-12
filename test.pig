/* 
	This entire example is referenced from - https://hortonworks.com/tutorial/how-to-process-data-with-apache-pig/#what-is-pig 
	Accessed on - 11/12/2017
	Comments by Anubhab Majumdar
*/

-- Load the drivers information
drivers = LOAD 'drivers.csv' USING PigStorage(',');
-- Remove header
raw_drivers = FILTER drivers BY $0>1;
-- Rename the fields
drivers_details = FOREACH raw_drivers GENERATE $0 AS driverId, $1 AS name;

-- Load the timesheet data
timesheet = LOAD 'timesheet.csv' USING PigStorage(',');
-- Remove header
raw_timesheet = FILTER timesheet by $0>1;
-- Rename the fields
timesheet_logged = FOREACH raw_timesheet GENERATE $0 AS driverId, $2 AS hours_logged, $3 AS miles_logged;

-- Group the timesheet data by driver ID
grp_logged = GROUP timesheet_logged by driverId;

-- Calculate the sum of hours and miles logged by each driver on the grouped data
sum_logged = FOREACH grp_logged GENERATE group as driverId,
SUM(timesheet_logged.hours_logged) as sum_hourslogged,
SUM(timesheet_logged.miles_logged) as sum_mileslogged;

-- JOIN the sum with details from driver dataset
join_sum_logged = JOIN sum_logged by driverId, drivers_details by driverId;

-- Rename the fields of JOIN
join_data = FOREACH join_sum_logged GENERATE $0 as driverId, $4 as name, $1 as hours_logged, $2 as miles_logged;

-- Return results
dump join_data;
