CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION postgis_sfcgal;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;


--DROP TABLE events_training_set;
CREATE TABLE events_training_set(event_id VARCHAR,event_description varchar,start_tstamp timestamp,confirmed_tstamp timestamp,created_tstamp timestamp,closed_tstamp timestamp,event_type varchar,event_subtype varchar,location varchar,latitude numeric,longitude numeric,number_of_responders numeric,lanes_affected numeric);
CREATE TABLE events_training_set(event_id VARCHAR,event_description VARCHAR,start_tstamp TIMESTAMP,confirmed_tstamp TIMESTAMP,created_tstamp TIMESTAMP,closed_tstamp VARCHAR,event_type VARCHAR,event_subtype VARCHAR,location VARCHAR,latitude numeric,longitude numeric,number_of_responders numeric,lanes_affected numeric);

COPY events_training_set FROM '/tmp/events_train.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM events_training_set WHERE closed_tstamp = 'accidentsAndIncidents'; 120.923 RECORDS
ALTER TABLE events_training_set ADD ID SERIAL;

SELECT * FROM events_training_set limit 1000;

DELETE FROM events_training_set WHERE longitude is null;
ALTER TABLE events_training_set ALTER COLUMN closed_tstamp TYPE TIMESTAMP;

ALTER TABLE events_training_set ALTER COLUMN closed_tstamp TYPE TIMESTAMP USING (closed_tstamp::TIMESTAMP);

CREATE INDEX IX_events_training_set_START_TSTAMP ON events_training_set USING BTREE(START_TSTAMP);
CREATE INDEX IX_events_training_set_CONFIRMED_TSTAMP ON events_training_set USING BTREE(CONFIRMED_TSTAMP);
CREATE INDEX IX_events_training_set_CREATED_TSTAMP ON events_training_set USING BTREE(CREATED_TSTAMP);
CREATE INDEX IX_events_training_set_CLOSED_TSTAMP ON events_training_set USING BTREE(CLOSED_TSTAMP);
CREATE INDEX IX_events_training_set_EVENT_TYPE ON events_training_set USING BTREE(EVENT_TYPE);
CREATE INDEX IX_events_training_set_EVENT_SUBTYPE ON events_training_set USING BTREE(EVENT_SUBTYPE);
ALTER TABLE events_training_set ADD COLUMN GEOM geometry;

ALTER TABLE events_training_set 
  ALTER COLUMN geom TYPE geometry(POINT, 4326) 
    USING ST_SetSRID(geom,4326);

UPDATE events_training_set SET GEOM = ST_SetSRID(ST_MAKEPOINT(LONGITUDE,LATITUDE),4326);

SELECT ST_SNAPTOGRID(GEOM,0.01), to_char(CREATED_TSTAMP, 'dy') AS DOW, event_type,event_subtype, COUNT(event_id)
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.01),to_char(CREATED_TSTAMP, 'dy') ORDER BY COUNT(event_id) DESC;


SELECT ST_SNAPTOGRID(GEOM,0.01), EXTRACT(HOUR FROM CREATED_TSTAMP) AS H, event_type,event_subtype, COUNT(event_id)
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.01), EXTRACT(HOUR FROM CREATED_TSTAMP) ORDER BY COUNT(event_id) DESC;


SELECT to_char(CREATED_TSTAMP, 'dy') AS DOW, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H, event_type,event_subtype, COUNT(event_id)
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents'
GROUP BY event_type,event_subtype, to_char(CREATED_TSTAMP, 'dy'), EXTRACT(HOUR FROM CREATED_TSTAMP) ORDER BY COUNT(event_id) DESC;


--DROP TABLE EVENTS_SNAPTOGRID_0_1_MON;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_MON
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'mon'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;

--DROP TABLE EVENTS_SNAPTOGRID_0_1_TUE;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_TUE
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'tue'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;

--DROP TABLE EVENTS_SNAPTOGRID_0_1_WED;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_WED
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'wed'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;

--DROP TABLE EVENTS_SNAPTOGRID_0_1_THU;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_THU
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'thu'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;

--DROP TABLE EVENTS_SNAPTOGRID_0_1_FRI;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_FRI
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'fri'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;


--DROP TABLE EVENTS_SNAPTOGRID_0_1_SAT;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_SAT
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'sat'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;


--DROP TABLE EVENTS_SNAPTOGRID_0_1_SUN;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_SUN
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'sun'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;


--DROP TABLE EVENTS_SNAPTOGRID_0_1_SUN;
SELECT ST_SNAPTOGRID(GEOM,0.1), to_char(CREATED_TSTAMP, 'dy') AS DOW, /*EXTRACT(HOUR FROM CREATED_TSTAMP) AS H,*/ event_type,event_subtype, COUNT(event_id)
INTO EVENTS_SNAPTOGRID_0_1_SUN
FROM events_training_set WHERE EVENT_TYPE = 'accidentsAndIncidents' AND to_char(CREATED_TSTAMP, 'dy') = 'sun'
GROUP BY event_type,event_subtype, ST_SNAPTOGRID(GEOM,0.1),to_char(CREATED_TSTAMP, 'dy')/*, EXTRACT(HOUR FROM CREATED_TSTAMP) AS H*/
HAVING COUNT(event_id) > 500
ORDER BY COUNT(event_id) DESC;


Copy (SELECT ROW_NUMBER() OVER(ORDER BY CREATED_TSTAMP ASC) AS ID, event_type, event_subtype, (EXTRACT('year' FROM CREATED_TSTAMP)||
(CASE WHEN EXTRACT('MONTH' FROM CREATED_TSTAMP)<10 THEN '0'||EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR ELSE EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR END))::INTEGER AS YEARMONTH
,LATITUDE,LONGITUDE FROM events_training_set WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL) To '/tmp/traffic_data.csv' With CSV;



SELECT ROW_NUMBER() OVER(ORDER BY CREATED_TSTAMP ASC) AS ID
,event_type
,event_subtype
,(EXTRACT('year' FROM CREATED_TSTAMP)||
(CASE WHEN EXTRACT('MONTH' FROM CREATED_TSTAMP)<10 THEN '0'||EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR ELSE EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR END))::INTEGER AS YEARMONTH
,ST_SETSRID(ST_Transform(ST_SETSRID(ST_MAKEPOINT(LONGITUDE,LATITUDE),4326),3857),3857) AS GEOM_3857
--,ST_Transform(ST_SETSRID(LONGITUDE,4326),3857)
INTO POINTS_3857
FROM events_training_set WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL;
CREATE INDEX IX_points_3857_GEOM_3857 ON points_3857 USING GIST(GEOM_3857);
--DROP TABLE POINTS_3857;
SELECT UpdateGeometrySRID('points_3857','geom_3857',3857);

CREATE TABLE POINTS_3857(ID SERIAL, event_type VARCHAR, event_subtype VARCHAR, CREATED_TSTAMP TIMESTAMP, YEARMONTH INTEGER, GEOM_3857 GEOMETRY(POINT,3857));

INSERT INTO POINTS_3857 (event_type,event_subtype,CREATED_TSTAMP,YEARMONTH,GEOM_3857)
(SELECT event_type
,event_subtype
,CREATED_TSTAMP
,(EXTRACT('year' FROM CREATED_TSTAMP)||
(CASE WHEN EXTRACT('MONTH' FROM CREATED_TSTAMP)<10 THEN '0'||EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR ELSE EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR END))::INTEGER 
,ST_SETSRID(ST_Transform(ST_SETSRID(ST_MAKEPOINT(LONGITUDE,LATITUDE),4326),3857),3857) 
FROM events_training_set WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL ORDER BY CREATED_TSTAMP ASC);

DELETE FROM POINTS_3857 WHERE ID IN (2668,4746,5468,6687);





SELECT ROW_NUMBER() OVER(ORDER BY CREATED_TSTAMP ASC) AS ID
,event_type
,event_subtype
,(EXTRACT('year' FROM CREATED_TSTAMP)||
(CASE WHEN EXTRACT('MONTH' FROM CREATED_TSTAMP)<10 THEN '0'||EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR ELSE EXTRACT('MONTH' FROM CREATED_TSTAMP)::VARCHAR END))::INTEGER AS YEARMONTH
,ST_SETSRID(ST_MAKEPOINT(LONGITUDE,LATITUDE),4326) AS GEOM_4326
--,ST_Transform(ST_SETSRID(LONGITUDE,4326),3857)
INTO POINTS_4326
FROM events_training_set WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL;
SELECT UpdateGeometrySRID('points_4326','geom_4326',4326);


SELECT * FROM POINTS_3857 LIMIT 10;

SELECT * FROM KmeansRoadwork LIMIT 10;
--drop table kmeansroadwork;
SELECT YEARMONTH, kmeans, count(*), ST_Centroid(ST_Collect(geom_3857)) AS geom
INTO KmeansRoadwork
FROM (
  SELECT M.ID,YEARMONTH, kmeans(ARRAY[ST_X(M.geom_3857), ST_Y(M.geom_3857)], 10) OVER (PARTITION BY M.YEARMONTH), M.geom_3857
  FROM POINTS_3857 M WHERE M.EVENT_TYPE =  'roadwork' 
) AS ksub
GROUP BY YEARMONTH, kmeans
ORDER BY YEARMONTH, kmeans;

SELECT UpdateGeometrySRID('kmeansroadwork','geom',3857);

CREATE TABLE KmeansRoadwork1(YeahMonth integer,KMeans integer, count integer, geom GEOMETRY(POINT,3857));
INSERT INTO KmeansRoadwork1(YeahMonth,KMeans, count, geom)
(SELECT YEARMONTH, kmeans, count(*), ST_Centroid(ST_Collect(geom_3857)) AS geom
 FROM (
  SELECT M.ID,YEARMONTH, kmeans(ARRAY[ST_X(M.geom_3857), ST_Y(M.geom_3857)], 10) OVER (PARTITION BY M.YEARMONTH), M.geom_3857
  FROM POINTS_3857 M WHERE M.EVENT_TYPE =  'roadwork' 
 ) AS ksub
 GROUP BY YEARMONTH, kmeans
 ORDER BY YEARMONTH, kmeans);


CREATE TABLE KmeansAccidentsAndIncidents(YeahMonth integer,KMeans integer, count integer, geom GEOMETRY(POINT,3857));
INSERT INTO KmeansAccidentsAndIncidents(YeahMonth,KMeans, count, geom)
(SELECT YEARMONTH, kmeans, count(*), ST_Centroid(ST_Collect(geom_3857)) AS geom
 FROM (
  SELECT M.ID,YEARMONTH, kmeans(ARRAY[ST_X(M.geom_3857), ST_Y(M.geom_3857)], 10) OVER (PARTITION BY M.YEARMONTH), M.geom_3857
  FROM POINTS_3857 M WHERE M.EVENT_TYPE =  'accidentsAndIncidents' 
 ) AS ksub
 GROUP BY YEARMONTH, kmeans
 ORDER BY YEARMONTH, kmeans);


 CREATE TABLE KmeansTrafficConditions(YeahMonth integer,KMeans integer, count integer, geom GEOMETRY(POINT,3857));
INSERT INTO KmeansTrafficConditions(YeahMonth,KMeans, count, geom)
(SELECT YEARMONTH, kmeans, count(*), ST_Centroid(ST_Collect(geom_3857)) AS geom
 FROM (
  SELECT M.ID,YEARMONTH, kmeans(ARRAY[ST_X(M.geom_3857), ST_Y(M.geom_3857)], 10) OVER (PARTITION BY M.YEARMONTH), M.geom_3857
  FROM POINTS_3857 M WHERE M.EVENT_TYPE =  'trafficConditions' 
 ) AS ksub
 GROUP BY YEARMONTH, kmeans
 ORDER BY YEARMONTH, kmeans);


  CREATE TABLE KmeansDeviceStatus(YeahMonth integer,KMeans integer, count integer, geom GEOMETRY(POINT,3857));
INSERT INTO KmeansDeviceStatus(YeahMonth,KMeans, count, geom)
(SELECT YEARMONTH, kmeans, count(*), ST_Centroid(ST_Collect(geom_3857)) AS geom
 FROM (
  SELECT M.ID,YEARMONTH, kmeans(ARRAY[ST_X(M.geom_3857), ST_Y(M.geom_3857)], 10) OVER (PARTITION BY M.YEARMONTH), M.geom_3857
  FROM POINTS_3857 M WHERE M.EVENT_TYPE =  'deviceStatus' 
 ) AS ksub
 GROUP BY YEARMONTH, kmeans
 ORDER BY YEARMONTH, kmeans);


  CREATE TABLE KmeansObstruction(YeahMonth integer,KMeans integer, count integer, geom GEOMETRY(POINT,3857));
INSERT INTO KmeansObstruction(YeahMonth,KMeans, count, geom)
(SELECT YEARMONTH, kmeans, count(*), ST_Centroid(ST_Collect(geom_3857)) AS geom
 FROM (
  SELECT M.ID,YEARMONTH, kmeans(ARRAY[ST_X(M.geom_3857), ST_Y(M.geom_3857)], 10) OVER (PARTITION BY M.YEARMONTH), M.geom_3857
  FROM POINTS_3857 M WHERE M.EVENT_TYPE =  'obstruction' 
 ) AS ksub
 GROUP BY YEARMONTH, kmeans
 ORDER BY YEARMONTH, kmeans);


SELECT MAX(COUNT) FROM KmeansRoadwork1;
SELECT Max(YEAHMONTH) FROM KmeansAccidentsAndIncidents;

COPY
(SELECT 'Obstruction',YEAHMONTH,KMEANS,COUNT,ST_X(ST_TRANSFORM(GEOM,4326)),ST_Y(ST_TRANSFORM(GEOM,4326)) FROM KmeansObstruction
	UNION (SELECT 'DeviceStatus', YEAHMONTH,KMEANS,COUNT,ST_X(ST_TRANSFORM(GEOM,4326)),ST_Y(ST_TRANSFORM(GEOM,4326)) FROM KmeansDeviceStatus)
	UNION (SELECT 'TrafficConditions', YEAHMONTH,KMEANS,COUNT,ST_X(ST_TRANSFORM(GEOM,4326)),ST_Y(ST_TRANSFORM(GEOM,4326)) FROM KmeansTrafficConditions)
	UNION (SELECT 'AccidentsAndIncidents', YEAHMONTH,KMEANS,COUNT,ST_X(ST_TRANSFORM(GEOM,4326)),ST_Y(ST_TRANSFORM(GEOM,4326)) FROM KmeansAccidentsAndIncidents)
	UNION (SELECT 'Roadwork', YEAHMONTH,KMEANS,COUNT,ST_X(ST_TRANSFORM(GEOM,4326)),ST_Y(ST_TRANSFORM(GEOM,4326)) FROM KmeansRoadwork1))
	TO '/tmp/traffic_events.csv' WITH CSV HEADER DELIMITER AS ';';

SELECT event_type,count(*) FROM POINTS_3857 group by event_type order by 2 desc;

SELECT * 
INTO POINTS_3857_accidentsAndIncidents
FROM POINTS_3857 WHERE EVENT_TYPE = 'accidentsAndIncidents';

SELECT * 
INTO POINTS_3857_trafficConditions
FROM POINTS_3857 WHERE EVENT_TYPE = 'trafficConditions';

SELECT * 
INTO POINTS_3857_roadwork
FROM POINTS_3857 WHERE EVENT_TYPE = 'roadwork';

SELECT * 
INTO POINTS_3857_deviceStatus
FROM POINTS_3857 WHERE EVENT_TYPE = 'deviceStatus';

SELECT * 
INTO POINTS_3857_obstruction
FROM POINTS_3857 WHERE EVENT_TYPE = 'obstruction';

SELECT EVENT_SUBTYPE,COUNT(ID) FROM POINTS_3857_roadwork GROUP BY EVENT_SUBTYPE ORDER BY 2 DESC;

SELECT * FROM POINTS_3857_obstruction LIMIT 10;

COPY (SELECT ID, EVENT_TYPE, EVENT_SUBTYPE,CREATED_TSTAMP, YEARMONTH, ST_X(ST_TRANSFORM(GEOM_3857,4326)) AS X,ST_Y(ST_TRANSFORM(GEOM_3857,4326)) AS Y 
FROM POINTS_3857_obstruction) TO '/tmp/obstruction_pts.csv' WITH CSV HEADER DELIMITER AS ';';

COPY (SELECT ID, EVENT_TYPE, EVENT_SUBTYPE,CREATED_TSTAMP, YEARMONTH
FROM POINTS_3857_roadwork) TO '/tmp/obstruction_bins.txt' with CSV HEADER;

COPY (SELECT ID, ST_X(ST_TRANSFORM(GEOM_3857,4326)) AS X,ST_Y(ST_TRANSFORM(GEOM_3857,4326)) AS Y
FROM POINTS_3857_roadwork) TO '/tmp/obstruction_xy.txt' with CSV HEADER;

SELECT EXTRACT(YEAR FROM CREATED_TSTAMP),EXTRACT(MONTH FROM CREATED_TSTAMP),EXTRACT(DAY FROM CREATED_TSTAMP),COUNT(ID) 
FROM POINTS_3857
WHERE EVENT_SUBTYPE = 'accident'
GROUP BY EXTRACT(YEAR FROM CREATED_TSTAMP),EXTRACT(MONTH FROM CREATED_TSTAMP),EXTRACT(DAY FROM CREATED_TSTAMP)
ORDER BY 4 DESC;

--DROP TABLE SUNSHINE_EFFECT;
SELECT E 
--INTO SUNSHINE_EFFECT
FROM events_training_set
WHERE EXTRACT(YEAR FROM CREATED_TSTAMP)=2014 AND EXTRACT(MONTH FROM CREATED_TSTAMP)=3 AND EXTRACT(DAY FROM CREATED_TSTAMP)=3 AND EVENT_TYPE = 'accidentsAndIncidents'
LIMIT 10;

SELECT event_subtype, event_subtype, /*EXTRACT(YEAR FROM CREATED_TSTAMP),EXTRACT(MONTH FROM CREATED_TSTAMP),EXTRACT(DAY FROM CREATED_TSTAMP),*/EXTRACT(hour FROM CREATED_TSTAMP), COUNT(ID) 
FROM POINTS_3857
GROUP BY event_subtype, event_subtype, /*EXTRACT(YEAR FROM CREATED_TSTAMP),EXTRACT(MONTH FROM CREATED_TSTAMP),EXTRACT(DAY FROM CREATED_TSTAMP),*/EXTRACT(hour FROM CREATED_TSTAMP)
ORDER BY COUNT(ID) DESC;

--DELETING POINTS FAR SOUTH TO REDUCE SIZE OF THE GRID
SELECT distinct ST_X(GEOM_3857),ST_Y(GEOM_3857),ST_Y(GEOM_3857)-lag(ST_Y(GEOM_3857)) OVER (ORDER BY ST_Y(GEOM_3857) asc) as y_diff 
FROM POINTS_3857 ORDER BY ST_Y(GEOM_3857) ASC
LIMIT 100;
SELECT * FROM POINTS_3857 WHERE ST_Y(GEOM_3857) < 4640401.33613995;
DELETE FROM POINTS_3857 WHERE ST_Y(GEOM_3857) < 4640401.33613995;

--CREATING 1KM CELL SIZE GRID AND FITTING POINTS FROM POINTS_3857 TO IT
DROP TABLE GRID_1KM;
CREATE TABLE GRID_1KM(ID SERIAL, GEOM GEOMETRY(POLYGON, 3857));
INSERT INTO GRID_1KM(GEOM)
(SELECT st_fishnet('public.points_3857','geom_3857',1000,3857));
CREATE INDEX IX_GRID_1KM_GEOM ON GRID_1KM USING GIST(GEOM);

--DROP TABLE GRID_1KM;
CREATE TABLE GRID_10KM(ID SERIAL, GEOM GEOMETRY(POLYGON, 3857));
INSERT INTO GRID_10KM(GEOM)
(SELECT st_fishnet('public.points_3857','geom_3857',10000,3857));
CREATE INDEX IX_GRID_10KM_GEOM ON GRID_10KM USING GIST(GEOM);

SELECT * FROM GRID_1KM LIMIT 10;

--ADDING 1KM GRID CELL_ID TO POINTS_3857
ALTER TABLE POINTS_3857 ADD CELL_ID INTEGER;
CREATE INDEX IX_POINTS_3857_GEOM_3857 ON POINTS_3857 USING GIST(GEOM_3857);
UPDATE POINTS_3857 A SET CELL_ID = (SELECT ID FROM GRID_10KM B WHERE B.GEOM&&A.GEOM_3857 AND ST_CONTAINS(B.GEOM,A.GEOM_3857)='t');

SELECT *,date_trunc('MONTH', CREATED_TSTAMP)::DATE FROM POINTS_3857 LIMIT 10;

--EXPORT FOR SatScan
--accidents
COPY(SELECT CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE AS DATE,COUNT(ID) 
     FROM POINTS_3857
     WHERE EVENT_TYPE = 'accidentsAndIncidents'
     GROUP BY CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE
     ORDER BY 1 ASC)
TO '/tmp/accidents_bins.txt' WITH CSV HEADER;

COPY(SELECT ID, ST_X(ST_CENTROID(ST_TRANSFORM(GEOM,4326))) AS X,ST_Y(ST_CENTROID(ST_TRANSFORM(GEOM,4326))) AS Y FROM GRID_10KM) TO '/tmp/grid_10km_xy.txt' WITH CSV HEADER; 

--roadwork
COPY(SELECT CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE AS DATE,COUNT(ID) 
     FROM POINTS_3857
     WHERE EVENT_TYPE = 'roadwork'
     GROUP BY CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE
     ORDER BY 1 ASC)
TO '/tmp/roadwork_bins.txt' WITH CSV HEADER;
SELECT * FROM POINTS_3857 WHERE EVENT_TYPE = 'roadwork'

SELECT * FROM GRID_1KM WHERE ACCIDENTS_CNT>0 LIMIT 100;
ALTER TABLE GRID_5KM ADD ACCIDENTS_CNT INT;
UPDATE GRID_5KM A SET ACCIDENTS_CNT = (SELECT COUNT(B.ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE = 'accidentsAndIncidents' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
SELECT ID,EVENT_TYPE,CREATED_TSTAMP,YEARMONTH,ST_X(GEOM_3857) AS X, ST_Y(GEOM_3857) AS Y FROM POINTS_3857; LIMIT 10;

ALTER TABLE POINTS_3857_accidentsAndIncidents ADD CellId1Km INT;
UPDATE POINTS_3857_accidentsAndIncidents A SET CellId1Km = (SELECT B.ID FROM GRID_1KM B WHERE B.GEOM&&A.GEOM_3857 AND ST_CONTAINS(B.GEOM,A.GEOM_3857)='t');

SELECT cellid1km,yearmonth,count(id) FROM POINTS_3857_accidentsAndIncidents GROUP BY cellid1km,yearmonth ORDER BY 3 DESC LIMIT 100;

SELECT cellid1km,count(id) FROM POINTS_3857_accidentsAndIncidents GROUP BY cellid1km ORDER BY 2 DESC LIMIT 20;

SELECT cellid1km,yearmonth,event_subtype, count(id) 
FROM POINTS_3857_accidentsAndIncidents
--WHERE cellid1km IN (SELECT cellid1km FROM POINTS_3857_accidentsAndIncidents GROUP BY cellid1km ORDER BY count(id) DESC LIMIT 5)
GROUP BY cellid1km,yearmonth,event_subtype ORDER BY 4 DESC;


SELECT * FROM NORTH_AMERICA_HIGHWAYS;
alter table NORTH_AMERICA_HIGHWAYS drop column geom_3857;
alter table NORTH_AMERICA_HIGHWAYS add geom_3857 geometry(multilinestring,3857);
update NORTH_AMERICA_HIGHWAYS set geom_3857 = st_transform(geom,3857);

--EXPORT FOR SatScan
--accidents
COPY(SELECT CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE AS DATE,COUNT(ID) 
     FROM POINTS_3857
     WHERE EVENT_TYPE = 'accidentsAndIncidents'
     GROUP BY CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE
     ORDER BY 1 ASC)
TO '/tmp/accidents_bins.txt' WITH CSV HEADER;

COPY(SELECT ID, ST_X(ST_CENTROID(ST_TRANSFORM(GEOM,4326))) AS X,ST_Y(ST_CENTROID(ST_TRANSFORM(GEOM,4326))) AS Y FROM GRID_10KM) TO '/tmp/grid_10km_xy.txt' WITH CSV HEADER; 

--roadwork
COPY(SELECT CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE AS DATE,COUNT(ID) 
     FROM POINTS_3857
     WHERE EVENT_TYPE = 'roadwork'
     GROUP BY CELL_ID, date_trunc('MONTH', CREATED_TSTAMP)::DATE
     ORDER BY 1 ASC)
TO '/tmp/roadwork_bins.txt' WITH CSV HEADER;

--ACCIDENTS RANDOM 0.05
--DROP TABLE POINTS_3857_Accidents_random_005
SELECT distinct id,date_trunc('MONTH', CREATED_TSTAMP)::DATE AS DATE, 1 AS COUNT, ST_X(ST_TRANSFORM(GEOM_3857,4326)) AS X,ST_Y(ST_TRANSFORM(GEOM_3857,4326)) AS Y
INTO POINTS_3857_Accidents_random_001
FROM POINTS_3857 WHERE EVENT_TYPE = 'accidentsAndIncidents'
AND RANDOM() < 0.01;
--SELECT * FROM POINTS_3857_Accidents_random_005 LIMIT 10;
COPY(SELECT ID, DATE,COUNT FROM POINTS_3857_Accidents_random_001 ORDER BY 1 ASC) TO '/tmp/accidents_bins_random_1_percent.txt' WITH CSV HEADER;
COPY(SELECT ID, X,Y FROM POINTS_3857_Accidents_random_001 ORDER BY 1 ASC) TO '/tmp/accidents_geom_random_1_percent.txt' WITH CSV HEADER;


--trafficConditions RANDOM 0.01
--DROP TABLE POINTS_3857_trafficConditions_random_001
SELECT distinct id,date_trunc('MONTH', CREATED_TSTAMP)::DATE AS DATE, 1 AS COUNT, ST_X(ST_TRANSFORM(GEOM_3857,4326)) AS X,ST_Y(ST_TRANSFORM(GEOM_3857,4326)) AS Y
INTO POINTS_3857_trafficConditions_random_01
FROM POINTS_3857 WHERE EVENT_TYPE = 'trafficConditions'
AND RANDOM() < 0.1;
--SELECT * FROM POINTS_3857_Accidents_random_005 LIMIT 10;
COPY(SELECT ID, DATE,COUNT FROM POINTS_3857_trafficConditions_random_01 ORDER BY 1 ASC) TO '/tmp/traffic_condition_bins_random_10_percent.txt' WITH CSV HEADER;
COPY(SELECT ID, X,Y FROM POINTS_3857_trafficConditions_random_01 ORDER BY 1 ASC) TO '/tmp/traffic_condition_geom_random_10_percent.txt' WITH CSV HEADER;
SELECT DISTINCT EVENT_SUBTYPE FROM POINTS_3857 WHERE EVENT_TYPE = 'trafficConditions' LIMIT 10;

SELECT ST_ASTEXT(GEOM), ST_ASTEXT(ST_CENTROID(GEOM))
FROM north_america_highways_dc_baltimore, 
WHERE FCC = 'A63' LIMIT 1;

CREATE INDEX IX_points_4326_GEOM ON points_4326 USING GIST(GEOM_4326);

SELECT A.ID,AVG(A.ACCIDENTS_CNT), AVG(ST_LENGTH(ST_Intersection(ST_TRANSFORM(a.geom,4326), b.GEOM)))
FROM grid_1km a, north_america_highways_dc_baltimore b
WHERE --A.ID IN (3034,410,3466) 
B.FCC='A63' 
AND ST_INTERSECTS(ST_TRANSFORM(a.geom,4326), b.GEOM)
GROUP BY A.ID;

SELECT ST_ASTEXT(ST_TRANSFORM(geom,4326)) FROM grid_1km WHERE ID IN (3034,410,3466);
SELECT ST_ASTEXT(GEOM) FROM north_america_highways_dc_baltimore WHERE FCC='A63';
SELECT count(distinct id) FROM GRID_1KM;
UPDATE GRID_5KM SET ID = ID::INTEGER;

SELECT * FROM GRID_5KM LIMIT 10;
ALTER TABLE GRID_5KM ADD ROADWORK_CNT INT; 
UPDATE GRID_5KM A SET ROADWORK_CNT = (SELECT COUNT(DISTINCT ID) FROM POINTS_3857 B WHERE EVENT_TYPE = 'roadwork' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
ALTER TABLE GRID_5KM ADD TrafficConditions_CNT INT; 
UPDATE GRID_5KM A SET TrafficConditions_CNT = (SELECT COUNT(DISTINCT ID) FROM POINTS_3857 B WHERE EVENT_TYPE = 'TrafficConditions_CNT' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
ALTER TABLE GRID_5KM ADD OBSTRUCTION_CNT INT; 
UPDATE GRID_5KM A SET OBSTRUCTION_CNT = (SELECT COUNT(DISTINCT ID) FROM POINTS_3857 B WHERE EVENT_TYPE = 'OBSTRUCTION' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
ALTER TABLE GRID_5KM ADD ROADWORK_CNT INT; 
UPDATE GRID_5KM A SET ROADWORK_CNT = (SELECT COUNT(DISTINCT ID) FROM POINTS_3857 B WHERE EVENT_TYPE = 'roadwork' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
SELECT * FROM POINTS_3857 LIMIT 100;
SELECT * FROM GRID_5KM LIMIT 10;
ALTER TABLE GRID_5KM ADD ID_IDT INTEGER; UPDATE GRID_5KM SET ID_IDT = ID;

SELECT distinct event_type FROM POINTS_3857 LIMIT 100;

CREATE EXTENSION tablefunc;

SELECT * FROM CROSSTAB(
'SELECT A.ID, YEARMONTH, EVENT_SUBTYPE, COUNT(B.ID)
 FROM GRID_1KM A, POINTS_3857 B WHERE A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)=''t'' AND EVENT_SUBTYPE IN (''disabled vehicle'',''accident'',''road maintenance operations'')
 GROUP BY A.ID, EVENT_SUBTYPE, YEARMONTH
 ORDER BY 1,2 ASC LIMIT 1000')
AS CT(ID INT,YEARMONTH INT, "disabled vehicle" INT,"accident" INT,"road maintenance operations" INT);

SELECT DISTINCT EVENT_SUBTYPE,COUNT(ID) FROM POINTS_3857 GROUP BY EVENT_SUBTYPE ORDER BY 2 DESC;
select * from POINTS_3857 limit 10;

SELECT A.ID, a.geom, EXTRACT('year' from created_tstamp) as y, event_type, count(b.id) INTO GRID_1KM_YM_TYPE
 FROM GRID_1KM A, POINTS_3857 B WHERE event_type in ('accidentsAndIncidents','obstruction','roadwork','trafficConditions') 
 and A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t'
 GROUP BY A.ID, a.geom, EVENT_TYPE, EXTRACT('year' from created_tstamp)
 ORDER BY 1,2 ASC;

--drop table GRID_1KM_YM_TYPE;
--drop table grid_1km_type_pivot;
select * from pivotmytable('grid_1km_ym_subtype','grid_1km_subtype_pivot','id,geom,yearmonth','event_subtype','count','sum',sort_order:='asc');
select * from pivotmytable('grid_1km_ym_type','grid_1km_type_pivot','id,geom,y','event_type','count','sum',sort_order:='asc');
select * from pivotedinfo limit 1;
select * from pivotedinfo limit 1;
SELECT DISTINCT EVENT_SUBTYPE FROM POINTS_3857 WHERE event_type in ('accidentsAndIncidents','obstruction','roadwork','trafficConditions');
select * from grid_1km_type_pivot limit 100;
select * from grid_1km limit 10;

ALTER TABLE GRID_1KM ADD ROADWORK_CNT INT;
UPDATE GRID_1KM A SET ROADWORK_CNT = (SELECT COUNT(B.ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE = 'roadwork' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
ALTER TABLE GRID_1KM ADD OBSTRUCTION_CNT INT;
UPDATE GRID_1KM A SET OBSTRUCTION_CNT = (SELECT COUNT(B.ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE = 'obstruction' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
ALTER TABLE GRID_1KM ADD TRAFFIC_CONDITIONS_CNT INT;
UPDATE GRID_1KM A SET TRAFFIC_CONDITIONS_CNT = (SELECT COUNT(B.ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE = 'trafficConditions' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');
select * from grid_1km order by 1 asc limit 10;
alter table grid_1km add UID int;
update grid_1km  set UID = id::int;

ALTER TABLE GRID_1KM ADD RAMP_LENGTH INT;
UPDATE GRID_1KM A SET RAMP_LENGTH = (SELECT COUNT(B.ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE = 'roadwork' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t');

SELECT ID,COUNT(DISTINCT GEOM) FROM GRID_1KM GROUP BY ID HAVING COUNT(DISTINCT GEOM) > 1;

SELECT DISTINCT B.ID,A.FCC, B.GEOM,A.GEOM, ST_LENGTH(ST_INTERSECTION(B.GEOM,ST_TRANSFORM(A.GEOM,3857))) 
FROM north_america_highways_dc_baltimore A, GRID_1KM B 
WHERE B.ID = 11672 --IN (109,111,113,114,116,118,120) 
AND ST_INTERSECTS(B.GEOM,ST_TRANSFORM(A.GEOM,3857))='t'
GROUP BY B.ID,A.FCC,B.GEOM,A.GEOM;

SELECT A.ID
      ,(SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,ST_TRANSFORM(GEOM,3857))='t') AS HIGHWAY_RAMP_CNT
      ,(SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t') AS HIGHWAY_RAMP_LENGTH
FROM GRID_1KM A WHERE A.ID=11671;
--ALTER TABLE north_america_highways_dc_baltimore DROP COLUMN GEOM_3857;
ALTER TABLE north_america_highways_dc_baltimore ADD GEOM_3857 GEOMETRY(MultiLineString, 3857);
UPDATE north_america_highways_dc_baltimore SET GEOM_3857 = ST_TRANSFORM(GEOM,3857);
CREATE INDEX IX_north_america_highways_dc_baltimore_GEOM_3857 ON north_america_highways_dc_baltimore USING GIST(GEOM_3857);

ALTER TABLE GRID_1KM DROP COLUMN RAMP_LENGTH;

ALTER TABLE GRID_1KM ADD HIGHWAY_RAMP_COUNT BIGINT;
ALTER TABLE GRID_1KM ADD HIGHWAY_RAMP_LENGTH BIGINT;
ALTER TABLE GRID_1KM ALTER COLUMN HIGHWAY_RAMP_LENGTH TYPE NUMERIC;
UPDATE GRID_1KM A SET HIGHWAY_RAMP_COUNT = CASE WHEN (SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t') IS NULL THEN 0
					    ELSE (SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t')
					    END;
UPDATE GRID_1KM A SET HIGHWAY_RAMP_LENGTH = CASE WHEN (SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t') IS NULL THEN 0
						ELSE (SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t')
						END;
ALTER TABLE north_america_detailed_streets_dc_baltimore ADD GEOM_3857 GEOMETRY(MULTILINESTRING, 3857);
UPDATE north_america_detailed_streets_dc_baltimore SET GEOM_3857 = ST_TRANSFORM(GEOM,3857);
CREATE INDEX IX_north_america_detailed_streets_dc_baltimore_GEOM_3857 ON north_america_detailed_streets_dc_baltimore USING GIST(GEOM_3857);

ALTER TABLE GRID_1KM ADD ROAD_LENGTH NUMERIC;
UPDATE GRID_1KM A SET ROAD_LENGTH = (SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_detailed_streets_dc_baltimore WHERE ST_INTERSECTS(A.GEOM,GEOM_3857)='t');

SELECT * FROM GRID_1KM WHERE ID = 11679;
SELECT COUNT(ID) FROM GRID_1KM WHERE ROAD_LENGTH IS NULL;

ALTER TABLE GRID_1KM ADD ACCIDENTS_CNT_LOG NUMERIC;
UPDATE GRID_1KM  SET ACCIDENTS_CNT_LOG = CASE WHEN ACCIDENTS_CNT = 0 THEN -100 ELSE LOG(ACCIDENTS_CNT) END;
CREATE TABLE DETECTOR_LANE_INVENTORY(
lane_id INT,
zone_id INT,
lane_number INT,
name varchar,
state varchar,
road varchar,
direction varchar,
location_description varchar,
lane_type varchar,
organization varchar,
detector_type varchar,
latitude NUMERIC,
longitude NUMERIC,
bearing NUMERIC,
default_speed INT,
interval INT);

COPY DETECTOR_LANE_INVENTORY FROM '/data/data/Tweets/traffic_project/detector_lane_inventory.csv' DELIMITER ',' CSV HEADER;
ALTER TABLE DETECTOR_LANE_INVENTORY ADD GEOM GEOMETRY(POINT,3857);
UPDATE DETECTOR_LANE_INVENTORY SET GEOM = ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(longitude,latitude),4326),3857);
CREATE INDEX IX_DETECTOR_LANE_INVENTORY_GEOM  ON DETECTOR_LANE_INVENTORY USING GIST(GEOM);
SELECT * FROM north_america_detailed_streets_dc_baltimore LIMIT 10;

SELECT *,LOG(ACCIDENTS_CNT) FROM GRID_1KM WHERE ACCIDENTS_CNT > 1000;
SELECT LOG(0.000000001);

SELECT ACCIDENTS_CNT,COUNT(DISTINCT ID) FROM GRID_1KM GROUP BY ACCIDENTS_CNT;
SELECT * FROM grid_1km_subtype_pivot LIMIT 10;

UPDATE POINTS_3857 SET EVENT_TYPE = REPLACE(EVENT_TYPE,' ','_');
UPDATE POINTS_3857 SET EVENT_SUBTYPE = REPLACE(EVENT_SUBTYPE,' ','_');

SELECT A.ID, a.geom, event_subtype, count(b.id) INTO GRID_1KM_SUBTYPE
 FROM GRID_1KM A, POINTS_3857 B WHERE event_type in ('accidentsAndIncidents','obstruction','roadwork','trafficConditions')  and 
 A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,B.GEOM_3857)='t'
 GROUP BY A.ID, a.geom, event_subtype
 ORDER BY 1,2 ASC;

--drop table GRID_1KM_SUBTYPE;
--drop table grid_1km_subtype_pivot_4_events;
select * from pivotmytable('grid_1km_subtype','grid_1km_subtype','id,geom','event_subtype','count','sum',sort_order:='asc');
DROP TABLE grid_1km_subtype_pivot;
SELECT * FROM grid_1km_subtype_pivot_4_events LIMIT 1;
SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'grid_1km_subtype_pivot_4_events';
select * from grid_1km_subtype limit 1000;
DO $$
DECLARE
	CUR1 CURSOR FOR SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'grid_1km_subtype_pivot_4_events_underscore' AND data_type = 'bigint';
	NEW_NAME VARCHAR;
BEGIN
	RAISE NOTICE 'PROCESSING STARTED...';
	FOR R IN CUR1 LOOP
		RAISE NOTICE 'OLD NAME: %, NEW NAME: %;',R.COLUMN_NAME,REPLACE(R.COLUMN_NAME,' ','_');
		--EXECUTE 'ALTER TABLE grid_1km_subtype_pivot_4_events RENAME COLUMN '||R.COLUMN_NAME||" TO "||REPLACE(R.COLUMN_NAME,' ','_')||";";
		EXECUTE 'ALTER TABLE grid_1km_subtype_pivot_4_events_underscore alter COLUMN '||R.COLUMN_NAME||' TYPE INT;';
		--EXECUTE 'ALTER TABLE LDA_FL_SUMMER_USERS DROP COLUMN IF EXISTS "'||r.NAME||'";ALTER TABLE LDA_FL_SUMMER_USERS ADD COLUMN "'||r.NAME||'" INTEGER;';
	END LOOP;
END$$;

SELECT replace('A B', 'cd', 'XX');
ALTER TABLE grid_1km_subtype_pivot_4_events RENAME COLUMN "numerous accidents" TO numerous_accidents; --too slow
select * from grid_1km_subtype_pivot_4_events_underscore limit 10;
update information_schema.columns set column_name = replace(column_name,' ','_') WHERE table_schema = 'public' AND table_name   = 'grid_1km_subtype_pivot_4_events'

select * from grid_5km limit 10;
SELECT * INTO GRID_1KM_WORKING FROM GRID_1KM;
ALTER TABLE GRID_5KM DROP COLUMN ACCIDENTS_CNT;
ALTER TABLE GRID_5KM DROP COLUMN ROADWORK_CNT;
ALTER TABLE GRID_5KM DROP COLUMN TRAFFICCONDITIONS_CNT;
ALTER TABLE GRID_5KM DROP COLUMN OBSTRUCTION_CNT;
ALTER TABLE GRID_5KM DROP COLUMN ID_IDT;
--DROP TABLE GRID_1KM_WORKING;

ALTER TABLE GRID_1KM ALTER COLUMN HIGHWAY_RAMP_LENGTH TYPE NUMERIC;
UPDATE GRID_1KM A SET HIGHWAY_RAMP_COUNT = CASE WHEN (SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t') IS NULL THEN 0
					    ELSE (SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t')
					    END;
DO $$
DECLARE
	CUR1 CURSOR FOR SELECT DISTINCT EVENT_TYPE FROM POINTS_3857 B;
BEGIN
	RAISE NOTICE 'PROCESSING STARTED...';
	FOR R IN CUR1 LOOP
		RAISE NOTICE 'PROCESSING %;',R.EVENT_TYPE;
		--EXECUTE 'ALTER TABLE GRID_1KM_WORKING ADD COLUMN '||R.EVENT_SUBTYPE||' INT;';
		EXECUTE 'ALTER TABLE GRID_5KM DROP COLUMN IF EXISTS "'||LEFT(r.EVENT_TYPE,16)||'";ALTER TABLE GRID_5KM ADD COLUMN "'||LEFT(r.EVENT_TYPE,16)||'" INTEGER;';
		EXECUTE 'UPDATE GRID_5KM A SET '||LEFT(r.EVENT_TYPE,16)||' = CASE WHEN (SELECT COUNT(ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE ='''||r.EVENT_TYPE||''' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,GEOM_3857)=''t'') IS NULL THEN 0
					    ELSE (SELECT COUNT(ID) FROM POINTS_3857 B WHERE B.EVENT_TYPE ='''||r.EVENT_TYPE||''' AND A.GEOM&&B.GEOM_3857 AND ST_CONTAINS(A.GEOM,GEOM_3857)=''t'')
					    END;';
	END LOOP;
END$$;

ALTER TABLE GRID_5KM ADD HIGHWAY_RAMP_COUNT BIGINT;
ALTER TABLE GRID_5KM ADD HIGHWAY_RAMP_LENGTH NUMERIC;

UPDATE GRID_5KM A SET HIGHWAY_RAMP_COUNT = CASE WHEN (SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t') IS NULL THEN 0
					    ELSE (SELECT COUNT(GID) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t')
					    END;
UPDATE GRID_5KM A SET HIGHWAY_RAMP_LENGTH = CASE WHEN (SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t') IS NULL THEN 0
						ELSE (SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_highways_dc_baltimore WHERE FCC = 'A63' AND ST_INTERSECTS(A.GEOM,GEOM_3857)='t')
						END;
ALTER TABLE north_america_detailed_streets_dc_baltimore ADD GEOM_3857 GEOMETRY(MULTILINESTRING, 3857);
UPDATE north_america_detailed_streets_dc_baltimore SET GEOM_3857 = ST_TRANSFORM(GEOM,3857);
CREATE INDEX IX_north_america_detailed_streets_dc_baltimore_GEOM_3857 ON north_america_detailed_streets_dc_baltimore USING GIST(GEOM_3857);

ALTER TABLE GRID_1KM ADD ROAD_LENGTH NUMERIC;
UPDATE GRID_1KM A SET ROAD_LENGTH = (SELECT SUM(ST_LENGTH(ST_INTERSECTION(A.GEOM,GEOM_3857))) FROM north_america_detailed_streets_dc_baltimore WHERE ST_INTERSECTS(A.GEOM,GEOM_3857)='t');
