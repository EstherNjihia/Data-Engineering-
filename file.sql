-- create table events( 
--     event_type integer not null, 
--     value integer not null, 
--     time timestamp not null, 
--     unique(event_type, time)
--   );


-- insert into events (event_type, value, time) VALUES(2, 5, '2015-05-09 12:42:00');

-- insert into events (event_type, value, time) VALUES(4 ,-42, '2015-05-09 13:19:57');
-- insert into events (event_type, value, time) VALUES(2, 2, '2015-05-09 14:48:30');
-- insert into events (event_type, value, time) VALUES(2, 7,  '2015-05-09 12:54:39');
-- insert into events (event_type, value, time) VALUES(3, 16, '2015-05-09 13:19:57');
-- insert into events (event_type, value, time) VALUES(3,20, '2015-05-09 15:01:09');


select a.event_type, a.previous_value - a.value as value from (
    select *, 
    LAG(value) OVER(PARTITION BY event_type ORDER BY time DESC) AS previous_value,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY time DESC) AS row_num from events)
    a 
    where row_num = 2
    ORDER BY event_type ASC;
