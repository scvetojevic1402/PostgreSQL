--for one of my projects I used panel data with standard error correction (PCSE package in R)
--panel data is convenient way of analyzing spatio-temporal data
--Cross-Section Panel Data enables spatio-temporal analysis to be modeled as simple GLM regression as long as variables are calculated properly
--GREAT PAPERS ABOUT CROSS SECTION DATA ANALYSIS
--Beck, N. (2001). Time-Series-Cross-Section Data: What have we learned in the last few years? Annual Review of Political Science, 4, 271–293. http://doi.org/10.1007/s13398-014-0173-7.2
--Beck, N., & Katz, J. N. (1996). Nuisance vs. Substance: Specifying and Estimating Time-Series-Cross-Section Models.
--Beck, N., & Katz, J. N. (1995). What to do (and not to do) with Time-Series Cross-Section Data. American Political Science Review. http://doi.org/10.2307/2082979
--https://cran.r-project.org/web/packages/pcse/index.html

---This is not a reproducible example, just a ton of somewhat organized code
/***************DATABASE CREATION*****************************/
/*CREATE DATABASE tgs7
  WITH OWNER = scvetojevic
       ENCODING = 'UTF8'
       TABLESPACE = tablespace1
       LC_COLLATE = 'en_US.UTF-8'
       LC_CTYPE = 'en_US.UTF-8'
       CONNECTION LIMIT = -1;

DROP DATABASE tgs7;
create extension dblink;*/

SELECT * FROM tweets_stream_sw LIMIT 10;

SELECT id
,replace(convert_from(decode,'UTF8'),'\u0000','')::json->>'id' AS TWEET_ID
,replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->'coordinates'->>0 AS X
,replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->'coordinates'->>1 AS Y
--,replace(convert_from(decode,'UTF8'),'\u0000','')::json->'entities'->'hashtags'
,json_array_elements((replace(convert_from(decode,'UTF8'),'\u0000','')::json->'entities'->'hashtags')::json)->>'text'
FROM dblink('dbname=twitter_geo_split_7 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_nw_ii limit 500000)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is NOT  null 
            and (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'entities'->>'hashtags') is not null;

CREATE TABLE ne_i(id int, tweet jsonb);
INSERT INTO ne_i
(SELECT id, replace(convert_from(decode,'UTF8'),'\u0000','')::jsonb
 FROM dblink('dbname=twitter_geo_split_7 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_nw_ii WHERE INSERT_TIME > ''2015-11-13 00:00:00.000000'' AND INSERT_TIME < ''2015-11-27 00:00:00.000000'')')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is NOT  null);



SELECT id, replace(convert_from(decode,'UTF8'),'\u0000','')::jsonb
 FROM dblink('dbname=twitter_geo_split_7 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_nw_ii limit 30)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is NOT  null;


SELECT * FROM tweets_stream_ne_i LIMIT 10;
ALTER TABLE tweets_stream_ne_i ALTER COLUMN JSON TYPE TEXT;

COPY tweets_stream_ne_i(id,insert_time,json) FROM '/mnt/tgs7/ne_i.csv' DELIMITER ',' CSV HEADER;

SELECT id
      ,insert_time
      ,replace(convert_from((decode(json,'base64')),'UTF8'),'\u0000','')::jsonb  as tweet
      INTO nw_i
FROM tweets_stream_nw_i;

CREATE INDEX IX_NW_I_TWEETS ON NW_I USING gin (TWEET);

CREATE TABLE ne_ii(id int, insert_time timestamp, tweet jsonb);

SELECT tweet->>'text' FROM NW_I WHERE tweet @> '{"lang": "fr"}' LIMIT 100;

SELECT TWEET->'id' FROM SE LIMIT 10;
--DROP TABLE ne_ii;


SELECT tweet->>'text',tweet->'entities'->'hashtags'->0->>'text',tweet->'entities'->'hashtags'->1->>'text',tweet->'entities'->'hashtags'->2->>'text',tweet->'entities'->'hashtags'->3->>'text'
FROM NW_I 
--WHERE tweet @> '{"hashtags": "Paris"}' 
LIMIT 10;
--CREATE EXTENSION postgis;
--CREATE EXTENSION postgis_topology;
--CREATE EXTENSION fuzzystrmatch;
--CREATE EXTENSION postgis_tiger_geocoder;
ALTER TABLE NE_II ADD GEOM GEOMETRY(POINT,3857);
ALTER TABLE NW_I ADD GEOM GEOMETRY(POINT,3857);
ALTER TABLE NW_II ADD GEOM GEOMETRY(POINT,3857);
ALTER TABLE NW_III ADD GEOM GEOMETRY(POINT,3857);
ALTER TABLE SE ADD GEOM GEOMETRY(POINT,3857);
ALTER TABLE SW ADD GEOM GEOMETRY(POINT,3857);

SELECT ID,--tweet->'place'->'name', tweet->'place'->'country',tweet->'place'->'place_type',tweet->'place'->'bounding_box'
tweet->'coordinates'->'coordinates',
ST_X(ST_MAKEPOINT((tweet->'coordinates'->'coordinates'->>0)::NUMERIC,(tweet->'coordinates'->'coordinates'->>1)::NUMERIC)),
ST_Y(ST_MAKEPOINT((tweet->'coordinates'->'coordinates'->>0)::NUMERIC,(tweet->'coordinates'->'coordinates'->>1)::NUMERIC)),
ST_X(ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT((tweet->'coordinates'->'coordinates'->>0)::NUMERIC,(tweet->'coordinates'->'coordinates'->>1)::NUMERIC),4326),3857)),
ST_Y(ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT((tweet->'coordinates'->'coordinates'->>0)::NUMERIC,(tweet->'coordinates'->'coordinates'->>1)::NUMERIC),4326),3857))
FROM NE_II 
WHERE tweet->'coordinates'->>'coordinates'!=''
LIMIT 1;

CREATE INDEX IX_NE_II_ID ON NE_II USING BTREE(ID);
UPDATE NE_II SET GEOM = ST_SETSRID(ST_MAKEPOINT((tweet->'coordinates'->'coordinates'->>0)::NUMERIC,(tweet->'coordinates'->'coordinates'->>1)::NUMERIC),3857) WHERE ID = 30026342;
CREATE EXTENSION POSTGIS;
CREATE TABLE PARIS_BB(GEOM GEOMETRY(POLYGON, 3857));

SELECT TWEET->'place'->>'name',tweet
FROM NE_I LIMIT 10;

SELECT st_astext(st_geomfromgeojson(tweet#>>'{coordinates}')) FROM NE_I WHERE GEOM IS NOT NULL LIMIT 10;

CREATE INDEX IX_NE_I_GEOM ON NE_I USING GIST(GEOM) WHERE GEOM IS NOT NULL;

SELECT ST_TRANSFORM(ST_ENVELOPE(ST_SETSRID(ST_MAKELINE(ST_MAKEPOINT(2.224122,48.791485),ST_MAKEPOINT(2.46976,48.920243)),4326)),3857);

--drop table paris_tweets;
SELECT * INTO PARIS_TWEETS
FROM NE_I 
--WHERE ST_TRANSFORM(ST_ENVELOPE(ST_SETSRID(ST_MAKELINE(ST_MAKEPOINT(2.224122,48.791485),ST_MAKEPOINT(2.46976,48.920243)),4326)),3857)&&GEOM
WHERE ST_TRANSFORM(ST_ENVELOPE(ST_SETSRID(ST_MAKELINE(ST_MAKEPOINT(1.9473,48.5948),ST_MAKEPOINT(2.9718,49.1008)),4326)),3857)&&GEOM
--UNION (SELECT * FROM NE_I WHERE TWEET->'place'->>'name' ILIKE 'PARIS%');

--INSERT INTO PARIS_TWEETS (SELECT * FROM NE_I WHERE TWEET->'place'->>'name' ILIKE 'PARIS%');

SELECT * INTO PLACE_PARIS_NO_XY
FROM NE_I WHERE TWEET->'place'->>'name' ILIKE 'PARIS%';


SELECT EXTRACT(MONTH FROM INSERT_TIME),EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME),COUNT(distinct ID)
FROM PARIS_TWEETS WHERE GEOM IS NOT NULL
GROUP BY EXTRACT(MONTH FROM INSERT_TIME),EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME)
ORDER BY 1,2,3 ASC;

SELECT * FROM PARIS_TWEETS LIMIT 100;
CREATE INDEX IX_PARIS_TWEETS_ID ON PARIS_TWEETS USING BTREE(ID);
CREATE INDEX IX_PARIS_TWEETS_INSTER_TIME ON PARIS_TWEETS USING BTREE(INSERT_TIME);

SELECT DISTINCT ID FROM PARIS_TWEETS;

SELECT st_fishnet('public.paris_tweets','geom',500,3857) AS GEOM INTO PARIS_FISHNET;
CREATE INDEX IX_PARIS_FISHNET_GEOM ON PARIS_FISHNET USING GIST(GEOM);

ALTER TABLE PARIS_TWEETS ADD CELL_ID INT;
ALTER TABLE PARIS_FISHNET ADD CELL_ID SERIAL;
UPDATE PARIS_TWEETS A SET CELL_ID = B.CELL_ID FROM PARIS_FISHNET B WHERE B.GEOM&&A.GEOM;

SELECT EXTRACT(DAY FROM INSERT_TIME) AS DD, EXTRACT(HOUR FROM INSERT_TIME) AS HH, CELL_ID, COUNT(DISTINCT ID)
INTO GRID_CELL_DD_HH
FROM PARIS_TWEETS
GROUP BY EXTRACT(DAY FROM INSERT_TIME), EXTRACT(HOUR FROM INSERT_TIME), CELL_ID
ORDER BY cell_id,EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME) ASC;

SELECT CELL_ID,stddev(COUNT) FROM GRID_CELL_DD_HH GROUP BY CELL_ID ORDER BY stddev(COUNT) DESC LIMIT 100;

SELECT * FROM GRID_CELL_DD_HH LIMIT 10;

ALTER TABLE PARIS_FISHNET ADD COUNT INT;
UPDATE PARIS_FISHNET U SET COUNT = C.CNT FROM (SELECT B.CELL_ID, COUNT(DISTINCT A.ID) AS CNT FROM PARIS_TWEETS A, PARIS_FISHNET B WHERE B.GEOM&&A.GEOM GROUP BY B.CELL_ID) C WHERE C.CELL_ID=U.CELL_ID;

SELECT TWEET#>>'{entities,urls,0,display_url}'
FROM PARIS_TWEETS --LIMIT 1000;
WHERE TWEET#>>'{entities,urls,0,display_url}' ilike '%instagram%';

ALTER TABLE PARIS_TWEETS ADD IMG_SRC VARCHAR;
UPDATE PARIS_TWEETS SET IMG_SRC = 'Twitter' WHERE TWEET#>>'{entities,media,0,type}'='photo';
UPDATE PARIS_TWEETS SET IMG_SRC = 'Instagram' WHERE TWEET#>>'{entities,urls,0,display_url}' ilike '%instagram%'; 

ALTER TABLE PARIS_FISHNET ADD TWITTER_IMG INT;
UPDATE PARIS_FISHNET U SET TWITTER_IMG = C.CNT FROM (SELECT B.CELL_ID, COUNT(DISTINCT A.ID) AS CNT FROM PARIS_TWEETS A, PARIS_FISHNET B WHERE A. IMG_SRC = 'Twitter' AND B.GEOM&&A.GEOM GROUP BY B.CELL_ID) C WHERE C.CELL_ID=U.CELL_ID;

ALTER TABLE PARIS_FISHNET ADD INSTA_IMG INT;
UPDATE PARIS_FISHNET U SET INSTA_IMG = C.CNT FROM (SELECT B.CELL_ID, COUNT(DISTINCT A.ID) AS CNT FROM PARIS_TWEETS A, PARIS_FISHNET B WHERE A. IMG_SRC = 'Instagram' AND B.GEOM&&A.GEOM GROUP BY B.CELL_ID) C WHERE C.CELL_ID=U.CELL_ID;

ALTER TABLE PARIS_TWEETS ADD USERNAME VARCHAR;
UPDATE PARIS_TWEETS U SET USERNAME = TWEET#>>'{user,name}';

SELECT TWEET#>>'{user,name}' FROM PARIS_TWEETS LIMIT 100;

SELECT USERNAME,COUNT(DISTINCT ID) FROM PARIS_TWEETS GROUP BY USERNAME ORDER BY 2 DESC;

SELECT TWEET#>>'{entities,media,0,expanded_url}',*
FROM PARIS_TWEETS WHERE CELL_ID = 1949;

CREATE INDEX IX_PARIS_TWEETS_TWEET ON PARIS_TWEETS USING GIN (TWEET);

SELECT DISTINCT TWEET#>>'{user,id}',TWEET#>>'{user,name}',TWEET#>>'{source}',MAX(TWEET#>>'{user,statuses_count}'), COUNT(ID) 
FROM PARIS_TWEETS 
GROUP BY TWEET#>>'{user,id}',TWEET#>>'{user,name}',TWEET#>>'{source}' ORDER BY 5 DESC; 
SELECT TWEET FROM PARIS_TWEETS LIMIT 1;

--DROP TABLE ATTACKS_DAYS;
SELECT DD,HH, SUM(COUNT) FROM GRID_CELL_DD_HH GROUP BY DD,HH ORDER BY 1,2 ASC;
SELECT * INTO ATTACKS_DAYS FROM PARIS_TWEETS WHERE INSERT_TIME < '2015-11-14 23:59:59.000000';
SELECT * FROM PARIS_TWEETS LIMIT 10;

DROP TABLE TWEET_SOURCE;
CREATE TABLE TWEET_SOURCE(ID SERIAL, SOURCE VARCHAR(500),COUNT INT);
INSERT INTO TWEET_SOURCE(SOURCE,COUNT)
(SELECT A.SRC,A.COUNT FROM (SELECT DISTINCT substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) AS SRC
	,COUNT(DISTINCT ID) 
	FROM PARIS_TWEETS WHERE TWEET#>>'{source}' !=''
	GROUP BY substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) 
	ORDER BY COUNT(DISTINCT ID) DESC) A);
	
ALTER TABLE PARIS_TWEETS ADD SOURCE_ID INT;
UPDATE PARIS_TWEETS SET SOURCE_ID = A.ID 
				    FROM TWEET_SOURCE A
				    WHERE TWEET#>>'{source}' != ''
				    AND SOURCE = substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1);

SELECT * FROM TWEET_SOURCE ORDER BY 1 ASC;
ALTER TABLE TWEET_SOURCE ADD ACCEPTABLE BOOLEAN;
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%instagram%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%android%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%ios%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%iphone%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%phone%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%Foursquare%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%ipad%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE SOURCE ILIKE '%blackberry%';
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'True' WHERE ID > 94;
UPDATE TWEET_SOURCE SET ACCEPTABLE = 'False' WHERE ACCEPTABLE IS NULL;

SELECT * FROM PARIS_TWEETS LIMIT 10;
SELECT CELL_ID,COUNT,TWITTER_IMG,INSTA_IMG FROM PARIS_FISHNET WHERE COUNT > 0;


SELECT USERNAME,EXTRACT(DAY FROM INSERT_TIME) AS DD,EXTRACT(HOUR FROM INSERT_TIME) AS HH,COUNT(ID),ST_AREA(ST_CONVEXHULL(ST_COLLECT(GEOM)))
FROM PARIS_TWEETS A
GROUP BY USERNAME,EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME)
ORDER BY 4 DESC;

UPDATE PARIS_FISHNET U SET COUNT = C.CNT FROM (SELECT B.CELL_ID, COUNT(DISTINCT A.ID) AS CNT FROM PARIS_TWEETS A, PARIS_FISHNET B, TWEET_SOURCE S 
					       WHERE A.SOURCE_ID=S.ID AND S.ACCEPTABLE = 't' AND B.GEOM&&A.GEOM GROUP BY B.CELL_ID) C WHERE C.CELL_ID=U.CELL_ID;
UPDATE PARIS_FISHNET U SET TWITTER_IMG = C.CNT FROM (SELECT B.CELL_ID, COUNT(DISTINCT A.ID) AS CNT FROM PARIS_TWEETS A, PARIS_FISHNET B, TWEET_SOURCE S 
						     WHERE A.SOURCE_ID=S.ID AND S.ACCEPTABLE = 't' AND A. IMG_SRC = 'Twitter' AND B.GEOM&&A.GEOM GROUP BY B.CELL_ID) C WHERE C.CELL_ID=U.CELL_ID;
UPDATE PARIS_FISHNET U SET INSTA_IMG = C.CNT FROM (SELECT B.CELL_ID, COUNT(DISTINCT A.ID) AS CNT FROM PARIS_TWEETS A, PARIS_FISHNET B, TWEET_SOURCE S 
						   WHERE A.SOURCE_ID=S.ID AND S.ACCEPTABLE = 't' AND A. IMG_SRC = 'Instagram' AND B.GEOM&&A.GEOM GROUP BY B.CELL_ID) C WHERE C.CELL_ID=U.CELL_ID;


SELECT * FROM PARIS_TWEETS LIMIT 10;

SELECT USERNAME,COUNT(DISTINCT T.ID)
FROM PARIS_TWEETS T, TWEET_SOURCE S
WHERE T.SOURCE_ID=S.ID AND S.ACCEPTABLE='t'
GROUP BY USERNAME
ORDER BY 2 DESC;

SELECT USERNAME,COUNT(ID),ST_AREA(ST_CONVEXHULL(ST_COLLECT(GEOM)))
FROM PARIS_TWEETS A
GROUP BY USERNAME
ORDER BY 2 DESC;

SELECT CELL_ID, TWEET#>>'{entities,media,0,media_url}',*
FROM PARIS_TWEETS T, TWEET_SOURCE S
WHERE T.SOURCE_ID=S.ID AND S.ACCEPTABLE='t'
AND TWEET#>>'{entities,media,0,type}'= 'photo' 
AND INSERT_TIME > '2015-11-13 20:00:00.887577' AND INSERT_TIME < '2015-11-15 08:00:00.887577'
ORDER BY 1 ASC 
LIMIT 10;

SELECT * INTO FIRST_PIC FROM PARIS_TWEETS WHERE ID = 8682087;
--Stade de France 8792954
SELECT DISTINCT TWEET#>>'{entities,media,0,media_url}',INSERT_TIME
                            FROM PARIS_TWEETS T, TWEET_SOURCE S
                            WHERE 
                            --T.SOURCE_ID=S.ID AND S.ACCEPTABLE='t' AND 
                            TWEET#>>'{entities,media,0,type}'= 'photo' 
                            AND INSERT_TIME < '2015-11-15 08:00:00.887577'
                            ORDER BY INSERT_TIME ASC;

SELECT * FROM PARIS_TWEETS WHERE TWEET#>>'{id}' = '665267239948566528';

SELECT DISTINCT TWEET#>>'{entities,media,0,media_url}',INSERT_TIME
                            FROM PLACE_PARIS_NO_XY T
                            WHERE 
                            --T.SOURCE_ID=S.ID AND S.ACCEPTABLE='t' AND 
                            TWEET#>>'{entities,media,0,type}'= 'photo' 
                            AND INSERT_TIME < '2015-11-14 08:00:00.887577'
                            ORDER BY INSERT_TIME ASC;

ALTER TABLE PLACE_PARIS_NO_XY ADD USEFUL BOOLEAN;
SELECT TWEET#>>'{retweet_count}',TWEET#>>'{favorite_count}' FROM PLACE_PARIS_NO_XY WHERE TWEET#>>'{id}' IN ('665297823441821697');
SELECT TWEET#>>'{retweet_count}',TWEET#>>'{favorite_count}' FROM PLACE_PARIS_NO_XY WHERE useful = 't';
SELECT * FROM PLACE_PARIS_NO_XY WHERE useful = 't' AND GEOM IS NOT NULL ORDER BY INSERT_TIME DESC;

SELECT TWEET#>>'{retweeted_status}' 
FROM ne_i 
WHERE TWEET#>>'{retweeted_status}' !=''
LIMIT 100;

CREATE INDEX IX_NE_I_ID ON NE_I USING BTREE(ID);
SELECT * FROM ne_i ORDER BY 1 ASC LIMIT 100;


SELECT (TWEET#>>'{created_at}')::timestamp, TWEET#>>'{entities,urls,0,expanded_url}',ID FROM PLACE_PARIS_NO_XY WHERE TWEET#>>'{entities,urls,0,expanded_url}' ILIKE '%insta%' AND GEOM IS NOT NULL AND (TWEET#>>'{created_at}')::timestamp BETWEEN '2015-11-13 20:00:00' AND '2015-11-14 06:00:00';
ALTER TABLE PLACE_PARIS_NO_XY ADD INSTA_URL VARCHAR;

CREATE TABLE LDA_HOURLY(ID SERIAL, DD INT, HH INT, LDA VARCHAR);

SELECT tweet, TWEET#>'{entities,hashtags}',
jsonb_object_keys(TWEET#>'{entities,hashtags,0}')
--jsonb_array_elements(TWEET#>'{entities,hashtags,0}')
FROM PLACE_PARIS_NO_XY cross join lateral jsonb_each_text(TWEET#>'{entities,hashtags}')
WHERE jsonb_array_length(TWEET#>'{entities,hashtags}') > 0 
LIMIT 1;

--DROP TABLE HASHTAGS;
SELECT * FROM HASHTAGS LIMIT 10;

SELECT TWEET_ID::BIGINT,J#>>'{text}' AS HASHTAG
INTO HASHTAGS 
FROM (SELECT TWEET#>>'{id}' AS TWEET_ID, jsonb_array_elements(TWEET#>'{entities,hashtags}') as J
	FROM PLACE_PARIS_NO_XY 
	WHERE jsonb_array_length(TWEET#>'{entities,hashtags}') > 0) AS A;
ALTER TABLE HASHTAGS ADD ID SERIAL;
SELECT ID,TWEET#>>'{text}' FROM PLACE_PARIS_NO_XY LIMIT 1000;

SELECT * FROM PLACE_PARIS_NO_XY LIMIT 100;
ALTER TABLE PLACE_PARIS_NO_XY ADD TWEET_ID BIGINT;
UPDATE PLACE_PARIS_NO_XY SET TWEET_ID = (TWEET#>>'{id}')::BIGINT;

SELECT EXTRACT(DAY FROM (TWEET#>>'{created_at}')::TIMESTAMP),EXTRACT(HOUR FROM (TWEET#>>'{created_at}')::TIMESTAMP),H.HASHTAG,COUNT(P.ID) 
FROM PLACE_PARIS_NO_XY P, HASHTAGS H 
WHERE P.TWEET_ID=H.TWEET_ID  AND TWEET#>>'{entities,urls,0,expanded_url}' NOT LIKE '%insta%' --AND GEOM IS NOT NULL
--AND P.USEFUL = 't'
GROUP BY EXTRACT(DAY FROM (TWEET#>>'{created_at}')::TIMESTAMP),EXTRACT(HOUR FROM (TWEET#>>'{created_at}')::TIMESTAMP), H.HASHTAG 
HAVING (COUNT(P.ID))>5
ORDER BY EXTRACT(DAY FROM (TWEET#>>'{created_at}')::TIMESTAMP),EXTRACT(HOUR FROM (TWEET#>>'{created_at}')::TIMESTAMP) ASC;

SELECT TWEET#>>'{entities,media,0,expanded_url}'
FROM PLACE_PARIS_NO_XY WHERE USEFUL = 'T' LIMIT 1;

ALTER TABLE PLACE_PARIS_NO_XY ADD RETWEETS INT;
ALTER TABLE PLACE_PARIS_NO_XY ADD FAVOURITES INT;

UPDATE PLACE_PARIS_NO_XY SET RETWEETS = 26, FAVOURITES = 2 WHERE ID = 8677671;
CREATE INDEX IX_PLACE_PARIS_NO_XY_ID ON PLACE_PARIS_NO_XY USING BTREE(ID);

SELECT ID,TWEET_ID, RETWEETS, FAVOURITES FROM PLACE_PARIS_NO_XY WHERE USEFUL='T';
UPDATE PLACE_PARIS_NO_XY SET RETWEETS = 12, FAVOURITES = 3 WHERE TWEET_ID = 665312904812494849;
UPDATE PLACE_PARIS_NO_XY SET FAVOURITES = 70 WHERE TWEET_ID =665266729141059584;
UPDATE PLACE_PARIS_NO_XY SET USEFUL = NULL, RETWEETS = null, FAVOURITES = null WHERE TWEET_ID = 665274296772730880;

CREATE TABLE RETWEETS(RETWEET_ID SERIAL, TWEET_ID BIGINT, USERNAME VARCHAR, USER_ID BIGINT);
SELECT * FROM RETWEETS;
SELECT TWEET_ID, RETWEETS FROM PLACE_PARIS_NO_XY WHERE RETWEETS > 0 AND RETWEETS < 23;
SELECT  FROM PLACE_PARIS_NO_XY WHERE USEFUL = 'T' LIMIT 10;

SELECT TWEET#>>'{entities,media,0,url}',* FROM PLACE_PARIS_NO_XY limit 1000
WHERE TWEET#>>'{entities,media,0,url}' ILIKE '%t.co%';

SELECT TWEET#>>'{created_at}',* FROM PLACE_PARIS_NO_XY WHERE TWEET_ID = 665314796380663808;
SELECT * FROM RETWEETS LIMIT 20;
ALTER TABLE RETWEETS RENAME TO RETWEETERS_HTML;

--DROP TABLE RETWEETS_API;
CREATE TABLE RETWEETS_API(RETWEET_ID SERIAL, TWEET_ID BIGINT, RETWEET VARCHAR);
ALTER DATABASE tgs7 SET TABLESPACE pg_default;
SELECT distinct tweet_id FROM RETWEETS_API;
SELECT * FROM RETWEETS_API;

SELECT DISTINCT A.TWEET_ID, RETWEETS 
FROM PLACE_PARIS_NO_XY A, RETWEETS_API B WHERE A.TWEET_ID=B.TWEET_ID
AND RETWEETS > 0 AND RETWEETS < 25 
ORDER BY A.TWEET_ID ASC;

SELECT TWEET_ID, RETWEETS FROM PLACE_PARIS_NO_XY A WHERE  RETWEETS BETWEEN 1 AND 25;
NOT EXISTS(SELECT TWEET_ID FROM RETWEETS_API WHERE TWEET_ID = A.TWEET_ID) AND

SELECT (RETWEET::jsonb)#>'{user,location}',(RETWEET::jsonb)#>'{user}', (RETWEET::jsonb)#>>'{user,screen_name}'
FROM RETWEETS_API LIMIT 20;

WHERE TWEET_ID = 665316491378556928;

SELECT DISTINCT TWEET_ID FROM RETWEETS_API

SELECT A.TWEET_ID, RETWEETS, COUNT(B.RETWEET_ID)
FROM PLACE_PARIS_NO_XY A, RETWEETS_API B WHERE A.TWEET_ID=B.TWEET_ID
AND RETWEETS > 0 AND RETWEETS < 25
GROUP BY A.TWEET_ID, RETWEETS
ORDER BY A.TWEET_ID ASC;

SELECT * FROM RETWEETS_API LIMIT 10;
ALTER TABLE RETWEETS_API ADD USER_ID BIGINT;
UPDATE RETWEETS_API SET USER_ID = ((RETWEET::jsonb)#>>'{user,id_str}')::BIGINT;

SELECT TWEET_ID,(TWEET#>>'{user,followers_count}')::INT,RETWEETS,FAVOURITES
FROM PLACE_PARIS_NO_XY WHERE USEFUL = 'T' ORDER BY 2 DESC;


--select tweet#>>'{geo}',tweet#>>'{place,name}' from ne_i limit 100;

select tweet#>>'{geo}',tweet#>>'{place,name}',(tweet#>>'{place,bounding_box,coordinates,0,0}')::numeric[]
from place_paris_no_xy limit 10;


SELECT ST_MakePolygon(ST_GeomFromText('LINESTRING(75.15 29.53 1,77 29 1,77.6 29.5 1, 75.15 29.53 1)'));

SELECT ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}'),tweet#>'{place,bounding_box}'
from place_paris_no_xy limit 10;

SELECT ST_UNION(ST_MAKEPOINT((tweet#>>'{place,bounding_box,coordinates,0,0,0}')::NUMERIC,(tweet#>>'{place,bounding_box,coordinates,0,0,1}')::NUMERIC)
               ,ST_MAKEPOINT((tweet#>>'{place,bounding_box,coordinates,0,1,0}')::NUMERIC,(tweet#>>'{place,bounding_box,coordinates,0,1,1}')::NUMERIC)
               ,ST_MAKEPOINT((tweet#>>'{place,bounding_box,coordinates,0,2,0}')::NUMERIC,(tweet#>>'{place,bounding_box,coordinates,0,2,1}')::NUMERIC)
               ,ST_MAKEPOINT((tweet#>>'{place,bounding_box,coordinates,0,3,0}')::NUMERIC,(tweet#>>'{place,bounding_box,coordinates,0,3,1}')::NUMERIC))
FROM place_paris_no_xy LIMIT 10;

SELECT ST_GeomFromText('MULTILINESTRING(((tweet#>>''{place,bounding_box,coordinates,0,0,0}'')::NUMERIC,(tweet#>>''{place,bounding_box,coordinates,0,0,1}'')::NUMERIC
			,(tweet#>>''{place,bounding_box,coordinates,0,1,0}'')::NUMERIC,(tweet#>>''{place,bounding_box,coordinates,0,1,1}'')::NUMERIC
			,(tweet#>>''{place,bounding_box,coordinates,0,2,0}'')::NUMERIC,(tweet#>>''{place,bounding_box,coordinates,0,2,1}'')::NUMERIC
			,(tweet#>>''{place,bounding_box,coordinates,0,3,0}'')::NUMERIC,(tweet#>>''{place,bounding_box,coordinates,0,3,1}'')::NUMERIC))'::geometry);

SELECT * 
FROM NE_I WHERE ST_ENVELOPE(ST_SETSRID(ST_MAKELINE(ST_MAKEPOINT(1.9473,48.5948),ST_MAKEPOINT(2.9718,49.1008)),4326))
			 &&ST_SETSRID(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}'),4326)
AND TWEET#>>'{place,name}' not ilike 'Paris' LIMIT 10;


CREATE INDEX ON NE_I USING GIN (tweet jsonb_path_ops);
CREATE INDEX ON NE_I ((tweet#>> '{place,bounding_box}'));
CREATE INDEX ON NE_I ((tweet#>> '{place,name}'));


SELECT * FROM place_paris_no_xy LIMIT 10;

--ALL TWEETS FROM PARIS---
SELECT st_astext(st_geomfromgeojson(tweet#>>'{coordinates}')), ST_ASTEXT(ST_TRANSFORM(GEOM,4326)) FROM SW WHERE GEOM IS NOT NULL LIMIT 10;

SELECT * INTO PARIS_ALL
FROM NE_I 
WHERE ST_ENVELOPE(ST_SETSRID(ST_MAKELINE(ST_MAKEPOINT(1.9473,48.5948),ST_MAKEPOINT(2.9718,49.1008)),4326)) &&ST_SETSRID(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}'),4326)
      OR ST_TRANSFORM(ST_ENVELOPE(ST_SETSRID(ST_MAKELINE(ST_MAKEPOINT(1.9473,48.5948),ST_MAKEPOINT(2.9718,49.1008)),4326)),3857)&&GEOM;

SELECT TWEET?|array['status'],TWEET#>>'{status,retweeted_status}',TWEET#>>'{status}',TWEET
FROM NE_I WHERE TWEET?|array['location']='t' LIMIT 1;

select count(id) from place_paris_no_xy;

SELECT TWEET#>>'{place,name}', COUNT(ID)
FROM PARIS_ALL
--WHERE TWEET#>>'{entities,media,0,type}'= 'photo'
GROUP BY TWEET#>>'{place,name}'
ORDER BY 2 DESC;

CREATE INDEX ON PARIS_ALL USING GIN (tweet jsonb_path_ops);

SELECT COUNT(ID) FROM PARIS_ALL WHERE TWEET?'retweeted_status'='t';
SELECT (RETWEET::jsonb)#>>'{retweeted_status}',retweet FROM RETWEETS_API LIMIT 100;
SELECT * FROM place_paris_no_xy LIMIT 10;
SELECT * FROM PARIS_ALL LIMIT 10;

ALTER TABLE PARIS_ALL ADD TWEET_ID BIGINT;
UPDATE PARIS_ALL SET TWEET_ID = ((TWEET::jsonb)#>>'{id}')::bigint;

ALTER TABLE PARIS_ALL ADD USEFUL BOOLEAN;
UPDATE PARIS_ALL A SET USEFUL = B.USEFUL FROM place_paris_no_xy B WHERE B.TWEET_ID=A.TWEET_ID;

ALTER TABLE PARIS_ALL ADD RETWEETS INTEGER;
UPDATE PARIS_ALL A SET RETWEETS = B.RETWEETS FROM place_paris_no_xy B WHERE B.TWEET_ID=A.TWEET_ID;

ALTER TABLE PARIS_ALL ADD FAVOURITES INTEGER;
UPDATE PARIS_ALL A SET FAVOURITES = B.FAVOURITES FROM place_paris_no_xy B WHERE B.TWEET_ID=A.TWEET_ID;

SELECT * FROM PARIS_ALL WHERE USEFUL IS NULL LIMIT 10;

SELECT TWEET#>>'{place,id}',TWEET FROM PARIS_ALL WHERE TWEET#>>'{place,name}' = 'Aubervilliers' LIMIT 1;

SELECT TWEET#>>'{coordaintes}' FROM PARIS_ALL WHERE TWEET?'coordaintes'='t' LIMIT 100;

SELECT * FROM PARIS_ALL LIMIT 10;
SELECT * FROM PARIS_ALL WHERE USEFUL = 't' AND NOT EXISTS (SELECT TWEET_ID FROM place_paris_no_xy WHERE TWEET_ID=PARIS_ALL.TWEET_ID);
SELECT INSERT_TIME FROM PARIS_ALL WHERE USEFUL='t' AND NOT EXISTS (SELECT TWEET_ID FROM place_paris_no_xy WHERE TWEET_ID=PARIS_ALL.TWEET_ID) ORDER BY 1 DESC LIMIT 1;

SELECT id, replace(convert_from(decode,'UTF8'),'\u0000','')::jsonb
 FROM dblink('dbname=twitter_geo_split_7 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_nw_ii limit 30)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is NOT  null;

SELECT * FROM PARIS_ALL WHERE RETWEETS > 0 AND NOT EXISTS (SELECT TWEET_ID FROM place_paris_no_xy WHERE TWEET_ID=PARIS_ALL.TWEET_ID);

SELECT * 
FROM RETWEETS_API 
LIMIT 50;

CREATE INDEX IX_PARIS_ALL_TWEET_ID ON PARIS_ALL USING BTREE(TWEET_ID);
CREATE INDEX IX_place_paris_no_xy_TWEET_ID ON place_paris_no_xy USING BTREE(TWEET_ID);

SELECT COUNT(ID) FROM place_paris_no_xy;
SELECT COUNT(DISTINCT ID) FROM PARIS_ALL WHERE EXISTS (SELECT ID FROM place_paris_no_xy WHERE TWEET_ID=PARIS_ALL.TWEET_ID);
SELECT * FROM PARIS_ALL A, place_paris_no_xy B WHERE A.TWEET_ID=B.TWEET_ID LIMIT 10;


SELECT A.TWEET_ID,RETWEETS,COUNT(B.RETWEET_ID)
FROM PARIS_ALL A, RETWEETS_API B
WHERE A.TWEET_ID=B.TWEET_ID
GROUP BY A.TWEET_ID,RETWEETS
ORDER BY 1 ASC;


CREATE INDEX IX_RETWEETS_API_USER_ID ON RETWEETS_API USING GIN(RETWEET->'user'->'id');

ALTER TABLE RETWEETS_API ALTER COLUMN RETWEET TYPE JSONB USING retweet::jsonb;
CREATE INDEX ON RETWEETS_API ((retweet#>> '{user,id}'));
CREATE INDEX ON NE_I ((tweet#>> '{user,id}'));
CREATE INDEX ON NE_II ((tweet#>> '{user,id}'));
CREATE INDEX ON NW_I ((tweet#>> '{user,id}'));
CREATE INDEX ON NW_II ((tweet#>> '{user,id}'));
CREATE INDEX ON NW_III ((tweet#>> '{user,id}'));
CREATE INDEX ON SE ((tweet#>> '{user,id}'));
CREATE INDEX ON SW ((tweet#>> '{user,id}'));

CREATE INDEX IX_RETWEETS_API_USER_ID ON RETWEETS_API USING GIN ((retweet->'user'->'id'));
CREATE INDEX IX_RETWEETS_API_USER_ID_BIGINT_BTREE ON RETWEETS_API USING BTREE (((retweet->'user'->>'id')::BIGINT));
CREATE INDEX IX_NE_I_USER_ID_BIGINT_BTREE ON NE_I USING BTREE (((tweet->'user'->>'id')::BIGINT));

SELECT RETWEET::JSONB FROM RETWEETS_API LIMIT 1000;

SET ENABLE_SEQSCAN=FALSE;

INSERT INTO TWEETS_OF_RETWEETERS
(
--EXPLAIN ANALYZE
SELECT B.ID, 'NE_II'::varchar AS TBL
 FROM RETWEETS_API A, NE_II B
 WHERE --ID < 30023722 AND 
 (((retweet -> 'user'::text) ->> 'id'::text))::bigint=(((B.tweet -> 'user'::text) ->> 'id'::text))::bigint
 );

SELECT ID FROM NE_II LIMIT 10;
select pg_get_indexdef(indexrelid) from pg_index where indrelid = 'NE_I'::regclass;
CREATE INDEX IX_NW_II_ID ON NW_II USING BTREE(ID);

SELECT distinct tbl FROM TWEETS_OF_RETWEETERS ORDER BY 1 ASC LIMIT 10;
SELECT ID FROM NE_I WHERE TWEET?'user' = 'f';
alter table tweets_of_retweeters alter column tbl type varchar;

DROP TABLE public.tweets_users_ne_i;
DROP TABLE public.tweets_users_ne_ii;
DROP TABLE public.tweets_users_nw_i;
DROP TABLE public.tweets_users_nw_ii;
DROP TABLE public.tweets_users_nw_iii;
DROP TABLE public.tweets_users_se;
DROP TABLE public.tweets_users_sw;

CREATE TABLE tweets_users_ne_i (tablename character varying,tableid integer,tweetid text,userid bigint);
CREATE TABLE tweets_users_ne_ii (tablename character varying,tableid integer,tweetid text,userid bigint);
CREATE TABLE tweets_users_nw_i (tablename character varying,tableid integer,tweetid text,userid bigint);
CREATE TABLE tweets_users_nw_ii (tablename character varying,tableid integer,tweetid text,userid bigint);
CREATE TABLE tweets_users_nw_iii (tablename character varying,tableid integer,tweetid text,userid bigint);
CREATE TABLE tweets_users_se (tablename character varying,tableid integer,tweetid text,userid bigint);
CREATE TABLE tweets_users_sw (tablename character varying,tableid integer,tweetid text,userid bigint);

DROP INDEX ne_ii_expr_idx;

select * from pg_stat_activity where datname = 'tgs7'

SELECT TWEET#>>'{user,screen_name}',TWEET#>>'{place,full_name}',TWEET#>>'{place,country_code}',TWEET#>>'{created_at}' FROM TWEETS_OF_RETWEETERS A, NE_I B
WHERE A.ID=B.ID
LIMIT 1000;

SELECT * FROM GEO_TAGGED_TWEETS_BY_RETWEETERS LIMIT 10;

SELECT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF
--INTO RETWEETERS_TWEETS_WITHIN_1_DAY
FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
WHERE A.USER_ID=B.USERID-- EXISTS (SELECT USER_ID FROM RETWEETS_API A WHERE A.USER_ID=B.USERID)
--AND (TWEET#>>'{created_at}')::timestamp
--AND EXTRACT(DAY FROM ((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp)) BETWEEN -1 AND 1
AND B.USERID IN (SELECT USER_ID FROM RETWEETS_API GROUP BY USER_ID HAVING COUNT(DISTINCT RETWEET_ID) =1)
LIMIT 10;

------------------------------------------------------------------VERY IMPORTANT-----------------------------------------------------------------------------------------------------------
ALTER TABLE GEO_TAGGED_TWEETS_BY_RETWEETERS ADD T_DIFF_FROM_RETWEET INTERVAL;
UPDATE GEO_TAGGED_TWEETS_BY_RETWEETERS SET T_DIFF_FROM_RETWEET = SUB.TIME_DIFF FROM (SELECT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
                                                 WHERE A.USER_ID=B.USERID AND B.USERID IN (SELECT USER_ID FROM RETWEETS_API GROUP BY USER_ID HAVING COUNT(DISTINCT RETWEET_ID) =1)) AS SUB
                                                 WHERE  GEO_TAGGED_TWEETS_BY_RETWEETERS.TWEETID = SUB.TWEETID;

ALTER TABLE GEO_TAGGED_TWEETS_BY_RETWEETERS ADD ORIGIN_TWEET_ID BIGINT;

CREATE INDEX IX_RETWEETS_USERS_CREATED_AT ON GEO_TAGGED_TWEETS_BY_RETWEETERS USING BTREE((TWEET->>'createed_at' ::text)::timestamp);
CREATE INDEX IX_RETWEETS_USERS_CREATED_AT ON GEO_TAGGED_TWEETS_BY_RETWEETERS USING BTREE((tweet ->> 'createed_at'::text)::timestamp);

SELECT COUNT(DISTINCT USER_ID) FROM RETWEETS_API;
SELECT COUNT(DISTINCT USERID) FROM GEO_TAGGED_TWEETS_BY_RETWEETERS;
SELECT COUNT(DISTINCT TWEET#>>'{user,id}') FROM PARIS_ALL WHERE USEFUL='t';

SELECT USER_ID, COUNT(DISTINCT RETWEET_ID) 
FROM RETWEETS_API
GROUP BY USER_ID
ORDER BY 2 DESC;
SELECT * FROM RETWEETS_API WHERE USER_ID = 220584725;

SELECT * FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE T_DIFF_FROM_RETWEET IS NOT NULL ORDER BY T_DIFF_FROM_RETWEET DESC LIMIT 100;

SELECT * FROM RETWEETS_API ORDER BY TWEET_ID ASC LIMIT 10;

--DROP TABLE FINAL_TABLE_AGG;
SELECT A.TWEET_ID, COALESCE(ST_TRANSFORM(A.GEOM,4326)::geometry(point,4326), A.PLACE_XY::geometry(point,4326)) AS GEOM,RETWEET
--,C.TWEET AS TWEET_BY_RETWEETER
--,ST_SETSRID(ST_GeomFromGeoJSON(c.tweet#>>'{place,bounding_box}'),4326)::geometry(Polygon,4326) AS RETWEETERS_TWEETS_PLACE
--,ST_CENTROID(ST_ENVELOPE(ST_SETSRID(ST_GeomFromGeoJSON(c.tweet#>>'{place,bounding_box}'),4326)))::geometry(point,4326) AS RETWEETERS_TWEETS_PLACE_CENTROID
--,ST_MAKELINE(COALESCE(ST_TRANSFORM(A.GEOM,4326)::geometry(point,4326), A.PLACE_XY::geometry(point,4326))
--             ,ST_SETSRID(ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(c.tweet#>>'{place,bounding_box}'))),4326))::geometry(Linestring,4326) AS RETWEET_LINE
--,ST_perimeter(ST_ENVELOPE(ST_TRANSFORM(ST_SETSRID(ST_GeomFromGeoJSON(c.tweet#>>'{place,bounding_box}'),4326),3857))) AS PLACE_POLYGON_PERIMETER
,(SELECT ST_SETSRID(ST_CENTROID(ST_ENVELOPE(ST_UNION(ST_POINT(ST_Y(ST_GeomFromGeoJSON(c.tweet#>>'{geo}')), ST_X(ST_GeomFromGeoJSON(c.tweet#>>'{geo}')))))),4326)::geometry(point,4326) AS XY 
  FROM GEO_TAGGED_TWEETS_BY_RETWEETERS C 
  WHERE B.USER_ID=C.USERID
  AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1 
  --AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) > -3 AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) < 3
  GROUP BY C.USERID) AS XY
 --,(SELECT c.tweet#>>'{user,screen_name}'  FROM GEO_TAGGED_TWEETS_BY_RETWEETERS C   WHERE B.USER_ID=C.USERID) AS USERNAME
--,ST_ASTEXT(ST_SETSRID(ST_POINT(ST_Y(ST_GeomFromGeoJSON(c.tweet#>>'{geo}')), ST_X(ST_GeomFromGeoJSON(c.tweet#>>'{geo}'))),4326)::geometry(point,4326)) AS XY_AS_TEXT
--,c.tweet#>>'{place,name}' AS PLACE_NAME
--,c.tweet#>>'{user,screen_name}' AS USERNAME
--,c.tweet#>>'{place,country}' AS COUNTRY
--,(c.tweet#>>'{user,friends_count}')::INT AS friends_count
--,(c.tweet#>>'{user,statuses_count}')::INT AS statuses_count
--,(c.tweet#>>'{user,followers_count}')::INT AS followers_count
INTO FINAL_TABLE_AGG
FROM PARIS_ALL A, RETWEETS_API B--, GEO_TAGGED_TWEETS_BY_RETWEETERS C
WHERE A.TWEET_ID=B.TWEET_ID --AND B.USER_ID=C.USERID
--GROUP BY A.TWEET_ID, COALESCE(ST_TRANSFORM(A.GEOM,4326)::geometry(point,4326), A.PLACE_XY::geometry(point,4326)),RETWEET
ORDER BY 1 ASC;
SELECT * 
FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B 
WHERE A.USER_ID=B.USERID
LIMIT 10;

---------------------------------------------VERY IMPORTANT RENAMING----------------------------------
ALTER TABLE RETWEETS_USERS RENAME TO GEO_TAGGED_TWEETS_BY_RETWEETERS;
---------------------------------------------VERY IMPORTANT RENAMING----------------------------------

ALTER TABLE PARIS_ALL ADD SUPPORT BOOLEAN;
SELECT * FROM PARIS_ALL WHERE TWEET_ID = 665292440425865216;
SELECT H.HASHTAG, 
FROM HASHTAGS H, PARIS_ALL A WHERE H.TWEET_ID=A.TWEET_ID AND A.USEFUL='T';


SELECT USERNAME,COUNT(*) FROM FINAL_TABLE_1 GROUP BY USERNAME ORDER BY 2 DESC 
WHERE PLACE_NAME = 'Le Havre';

ALTER TABLE PARIS_ALL ADD PLACE_XY GEOMETRY(POINT, 4326);
UPDATE PARIS_ALL SET PLACE_XY = ST_CENTROID(ST_SETSRID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')),4326))::GEOMETRY(POINT,4326);
DROP TABLE A1;
SELECT ST_CENTROID(ST_SETSRID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')),4326))::GEOMETRY(POINT,4326) INTO A1 FROM PARIS_ALL WHERE TWEET_ID = 665276392368680960;
SELECT ST_SETSRID(ST_POINT(ST_Y(ST_GeomFromGeoJSON(tweet#>>'{geo}')), ST_X(ST_GeomFromGeoJSON(tweet#>>'{geo}'))),4326)::geometry(point,4326) AS XY
FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
LIMIT 10;  
--

select st_astext(xy),*
from final_table where xy is not null and place_name = 'Democratic Republic of Congo'
limit 10;

SELECT substring(tweet_by_retweeter#>>'{source}',position('>' in tweet_by_retweeter#>>'{source}')+1,position('</' in tweet_by_retweeter#>>'{source}')-position('>' in tweet_by_retweeter#>>'{source}')-1),COUNT(*) 
FROM final_table_1
GROUP BY substring(tweet_by_retweeter#>>'{source}',position('>' in tweet_by_retweeter#>>'{source}')+1,position('</' in tweet_by_retweeter#>>'{source}')-position('>' in tweet_by_retweeter#>>'{source}')-1)
ORDER BY 2 DESC;


SELECT USERNAME,COUNTRY,MAX(statuses_count), COUNT(*) 
FROM FINAL_TABLE
GROUP BY USERNAME,COUNTRY
ORDER BY COUNT(*) DESC;

SELECT st_x(xy),st_y(xy),st_astext(xy),tweet_by_retweeter FROM FINAL_TABLE WHERE PLACE_NAME = 'Hamburg' LIMIT 20
WHERE statuses_count < 1000;


SELECT * FROM FINAL_TABLE_1
WHERE substring(tweet_by_retweeter#>>'{source}',position('>' in tweet_by_retweeter#>>'{source}')+1,position('</' in tweet_by_retweeter#>>'{source}')-position('>' in tweet_by_retweeter#>>'{source}')-1)
      IN ('Twitter for Android','Twitter for iPhone');

SELECT COUNT(*) FROM PARIS_ALL WHERE SUPPORT = 'T' LIMIT 10;

SELECT * FROM PARIS_ALL WHERE USEFUL = 'T' AND RETWEETS < 21 LIMIT 10;

SELECT *, (SELECT RETWEETS FROM PARIS_ALL WHERE TWEET_ID = A.TWEET_ID) as t
FROM FINAL_TABLE_1 A
LIMIT 10;

ALTER TABLE FINAL_TABLE_1 ADD RETWEETS INT;
ALTER TABLE FINAL_TABLE_1 ADD FAVORITES INT;
UPDATE FINAL_TABLE_1 A SET RETWEETS = B.RETWEETS FROM PARIS_ALL B WHERE B.TWEET_ID = A.TWEET_ID;
UPDATE FINAL_TABLE_1 A SET FAVORITES = B.FAVOURITES FROM PARIS_ALL B WHERE B.TWEET_ID = A.TWEET_ID;

RETWEETERS_TWEETS_PLACE
SELECT USERNAME,COUNT(DISTINCT XY),COUNT(*)
FROM FINAL_TABLE_1
GROUP BY USERNAME
ORDER BY 2 DESC;

SELECT * FROM PARIS_ALL LIMIT 10;
SELECT EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME),COUNT(*) 
FROM PARIS_ALL
GROUP BY EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME)
ORDER BY 1,2 ASC;

SELECT distinct RETWEET#>'{retweeted_status,id}' AS ORIGINAL_TWEET_ID, RETWEET#>'{user,screen_name}' AS SCREEN_NAME,RETWEET#>'{user,id}' AS USER_ID, RETWEET#>'{user,statuses_count}' AS STATUS_COUNT,RETWEET#>'{id}' AS RETWEET_ID, RETWEET#>'{user,description}', country
FROM FINAL_TABLE_1 
--WHERE COUNTRY = 'Brasil' 
ORDER BY COUNTRY --RETWEET#>'{user,statuses_count}' ASC LIMIT 50; USERNAME = 'jornalistavitor';

SELECT RETWEET#>'{retweeted_status,user,screen_name}' AS ORIGINAL_USER_ID,RETWEET#>'{retweeted_status,id}' AS ORIGINAL_TWEET_ID,COUNTRY,COUNT(DISTINCT RETWEET#>'{id}')
FROM FINAL_TABLE_1 
--WHERE COUNTRY = 'Brasil' 
GROUP BY RETWEET#>'{retweeted_status,user,screen_name}',RETWEET#>'{retweeted_status,id}',COUNTRY
ORDER BY 4 DESC
CREATE INDEX IX_PARIS_ALL_USEFUL ON PARIS_ALL USING BTREE(USEFUL);

SELECT distinct TWEET#>'{user,screen_name}', TWEET#>'{user,followers_count}' FROM PARIS_ALL WHERE USEFUL = 't' ORDER BY 2 DESC LIMIT 100;
SELECT distinct TWEET#>'{user,screen_name}' FROM PARIS_ALL WHERE USEFUL = 't';
SELECT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id FROM PARIS_ALL A WHERE USEFUL = 't' AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) LIMIT 1;
SELECT user_id,count(follower_id) FROM FOLLOWERS group by user_id ORDER BY 2 DESC;
SELECT TWEET#>'{place,country}',TWEET#>'{created_at}',* FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE USERID = 277717058 ORDER BY T_DIFF_FROM_RETWEET ASC;
SELECT tweet_id,count(distinct retweet)
 FROM RETWEETS_API
 GROUP BY TWEET_ID
 ORDER BY 2 DESC ;

 SELECT RETWEET#>'{user,screen_name}',RETWEET#>'{user,id}', RETWEET#>'{user,statuses_count}',RETWEET#>'{id}',*
FROM RETWEETS_API WHERE TWEET_ID = 665291860093640704;

SELECT * FROM FINAL_TABLE_1 WHERE USERNAME = 'Myriam_u_know';
SELECT * FROM GEO_TAGGED_TWEETS_BY_RETWEETERS LIMIT 10 WHERE USER_ID = 1964312245;

 SELECT RETWEET#>'{user,screen_name}',RETWEET#>'{user,id}', RETWEET#>'{user,statuses_count}',RETWEET#>'{id}'
FROM FINAL_TABLE_1 LIMIT 100;

 SELECT TWEET#>'{place,country}',TWEET#>'{user,screen_name}',*
 FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE USERID=19720364;

SELECT tweet#>'{user,id}',* FROM NW_I WHERE ID = 19356607;

SELECT *
FROM RETWEETS_API A, tweetsusers B 
WHERE  A.USER_ID=B.USERID AND b.USERID=19356607;

CREATE TABLE FOLLOWERS(ID SERIAL, USER_ID BIGINT, FOLLOWER_ID BIGINT);

CREATE TABLE retweets_api_NEW (retweet_id SERIAL,tweet_id bigint,retweet jsonb,user_id bigint);

SELECT retweet#>'{created_at}',retweet#>'{text}',retweet#>'{retweeted_status,user,screen_name}',RETWEET
FROM RETWEETS_API WHERE (RETWEET#>>'{text}')::VARCHAR ILIKE '%RT%@%@%';

SELECT DISTINCT TWEET#>>'{user,id}',RETWEETS  FROM PARIS_ALL WHERE  USEFUL='t' AND EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (TWEET#>>'{user,id}')::BIGINT)  AND RETWEETS BETWEEN 5 AND 15 LIMIT 10;
SELECT * FROM FOLLOWERS WHERE USER_ID = 4228459939;

SELECT A.TWEET_ID,F.USER_ID,F.FOLLOWER_ID
FROM RETWEETS_API A, FOLLOWERS_OF_FOLLOWERS F 
WHERE A.USER_ID=F.FOLLOWER_ID;

ALTER TABLE FOLLOWERS ADD RETWEETER BOOLEAN;
UPDATE FOLLOWERS SET RETWEETER = 't' WHERE EXISTS (SELECT * FROM RETWEETS_API WHERE USER_ID = FOLLOWERS.FOLLOWER_ID);
UPDATE FOLLOWERS SET RETWEETER = 'f' WHERE NOT EXISTS (SELECT * FROM RETWEETS_API WHERE USER_ID = FOLLOWERS.FOLLOWER_ID);
ALTER TABLE FOLLOWERS ADD FOLLOWER_SCREEN_NAME VARCHAR;
UPDATE FOLLOWERS SET FOLLOWER_SCREEN_NAME = (RETWEET#>>'{user,screen_name}')::VARCHAR FROM RETWEETS_API WHERE RETWEETS_API.USER_ID = FOLLOWERS.FOLLOWER_ID AND RETWEETER = 't';
SELECT (RETWEET#>>'{user,followers_count}')::INT FROM RETWEETS_API WHERE USER_ID IN (SELECT FOLLOWER_ID FROM FOLLOWERS) ORDER BY 1 DESC;
SELECT COUNT(*) FROM FOLLOWERS;

SELECT distinct FOLLOWER_SCREEN_NAME AS user_name, FOLLOWER_ID AS user_id FROM FOLLOWERS WHERE RETWEETER = 't'
           AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS_OF_FOLLOWERS FOF WHERE FOF.USER_ID = FOLLOWERS.FOLLOWER_ID);
           
SELECT * FROM FOLLOWERS WHERE RETWEETER = 't';
CREATE TABLE FOLLOWERS_OF_FOLLOWERS(ID SERIAL, USER_ID BIGINT, FOLLOWER_ID BIGINT);
SELECT * FROM PARIS_ALL WHERE USEFUL='t' AND TWEET#>>'{user,screen_name}'='zeitonlinesport';
SELECT distinct FOLLOWER_SCREEN_NAME AS user_name, FOLLOWER_ID AS user_id FROM FOLLOWERS WHERE RETWEETER = 't';
SELECT * FROM FOLLOWERS_OF_FOLLOWERS;

SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id FROM PARIS_ALL A WHERE USEFUL = 't' AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND RETWEETS>0;
SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id
             FROM PARIS_ALL A WHERE USEFUL = 't' AND NOT EXISTS
             (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND RETWEETS > 0;

SELECT * FROM FOLLOWERS_OF_FOLLOWERS WHERE FOLLOWER_ID = 0;

-----------------------------------------------------------------------------------------------------------------------------VERY IMPORTANT!!! BTREE INDEX USED IN JSON------------------------------------------------------------------------------------------------------------------------
EXPLAIN ANALYZE 
SELECT COUNT(ID) FROM NW_III WHERE  ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS_OF_FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID));

SELECT * FROM GEO_TAGGED_TWEETS_BY_RETWEETERS LIMIT 10;
ALTER TABLE GEO_TAGGED_TWEETS_BY_RETWEETERS ADD DEPTH INT; UPDATE GEO_TAGGED_TWEETS_BY_RETWEETERS SET DEPTH = 1;

INSERT INTO GEO_TAGGED_TWEETS_BY_RETWEETERS (SELECT 'SE',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,2 FROM SE 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS_OF_FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));

SELECT TABLENAME,COUNT(TABLEID) 
FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
WHERE DEPTH = 2
GROUP BY TABLENAME ORDER BY 1 ASC;
SELECT COUNT(TABLEID)  FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE DEPTH = 2;
-----------------------------------------------------------------------------------------------------------------------------TIME INTERVAL BETWEEN RETWEET AND GEO-TAGGED TWEETS UPDATE------------------------------------------------------------------------------------------------------------------------
UPDATE GEO_TAGGED_TWEETS_BY_RETWEETERS SET T_DIFF_FROM_RETWEET = SUB.TIME_DIFF 
		FROM (SELECT DISTINCT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF 
			  FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
                                                 WHERE B.DEPTH=1 AND A.USER_ID=B.USERID ) AS SUB
                                                 WHERE  GEO_TAGGED_TWEETS_BY_RETWEETERS.TWEETID = SUB.TWEETID AND GEO_TAGGED_TWEETS_BY_RETWEETERS.DEPTH=1;
UPDATE GEO_TAGGED_TWEETS_BY_RETWEETERS SET T_DIFF_FROM_RETWEET = SUB.TIME_DIFF 
		FROM (SELECT DISTINCT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF 
			  FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
                                                 WHERE A.TWEET_TYPE='SUPPORT' AND A.USER_ID=B.USERID ) AS SUB
                                                 WHERE  GEO_TAGGED_TWEETS_BY_RETWEETERS.TWEETID = SUB.TWEETID AND GEO_TAGGED_TWEETS_BY_RETWEETERS.DEPTH=1;

UPDATE GEO_TAGGED_TWEETS_BY_RETWEETERS SET T_DIFF_FROM_RETWEET = SUB.TIME_DIFF 
		FROM (SELECT DISTINCT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF 
			  FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
                                                 WHERE A.TWEET_TYPE is null AND A.USER_ID=B.USERID ) AS SUB
                                                 WHERE  GEO_TAGGED_TWEETS_BY_RETWEETERS.TWEETID = SUB.TWEETID AND GEO_TAGGED_TWEETS_BY_RETWEETERS.DEPTH=1;
                                                 
---AND B.USERID IN (SELECT USER_ID FROM RETWEETS_API GROUP BY USER_ID HAVING COUNT(DISTINCT RETWEET_ID) =1) *********************************** KO ZNA ZASTO SAM OVO KORISTIO U PRETHODNOJ VERZIJI
SELECT COUNT(*) FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE DEPTH=2 AND T_DIFF_FROM_RETWEET IS NULL;
SELECT USER_ID,COUNT(DISTINCT RETWEET_ID) FROM RETWEETS_API GROUP BY USER_ID HAVING COUNT(DISTINCT RETWEET_ID) =1
SELECT * FROM RETWEETS_API WHERE USER_ID = 394332805;


SELECT COUNT(*) FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) BETWEEN -1 AND 1 AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) BETWEEN -3 AND 3;

SELECT A.TWEET_ID, COALESCE(ST_TRANSFORM(A.GEOM,4326)::geometry(point,4326), A.PLACE_XY::geometry(point,4326)) AS GEOM,RETWEET
,(SELECT ST_SETSRID(ST_CENTROID(ST_ENVELOPE(ST_UNION(ST_POINT(ST_Y(ST_GeomFromGeoJSON(c.tweet#>>'{geo}')), ST_X(ST_GeomFromGeoJSON(c.tweet#>>'{geo}')))))),4326)::geometry(point,4326) AS XY 
     FROM GEO_TAGGED_TWEETS_BY_RETWEETERS C 
     WHERE B.USER_ID=C.USERID  AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1 AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) > -3 AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) < 3
     GROUP BY C.USERID) AS XY
INTO FINAL_TABLE_AGG_D2
FROM PARIS_ALL A, RETWEETS_API B WHERE A.TWEET_ID=B.TWEET_ID ORDER BY 1 ASC;

SELECT USERID, DEPTH, ST_SETSRID(ST_CENTROID(ST_ENVELOPE(ST_UNION(ST_POINT(ST_Y(ST_GeomFromGeoJSON(tweet#>>'{geo}')), ST_X(ST_GeomFromGeoJSON(tweet#>>'{geo}')))))),4326)::geometry(point,4326) AS XY
--,(SELECT COALESCE(geom,PLACE_XY) FROM PARIS_ALL WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) = GEO_TAGGED_TWEETS_BY_RETWEETERS.USERID LIMIT 1)
     FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
     WHERE EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1 --AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) > -3 AND EXTRACT(HOUR FROM T_DIFF_FROM_RETWEET) < 3
	       AND DEPTH=2
     GROUP BY USERID, DEPTH
     ORDER BY 1,2 ASC;

    SELECT ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{geo}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326)
    FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
    WHERE EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1 
    LIMIT 10;

    ----SELEDECI BITAN KORAK: POVEZATI GEO-TAGGED TWEET SA RETWEETOM I ORIGINALNIM TWEETOM... 
    SELECT RA.TWEET_ID,RA.RETWEET#>>'{id}'
    FROM RETWEETS_API RA, GEO_TAGGED_TWEETS_BY_RETWEETERS G
    WHERE G.USERID=(RA.RETWEET#>>'{user,id}')::bigint and g.userid = 2753743832 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1;
    SELECT RETWEET#>>'{created_at}',*
    FROM RETWEETS_API WHERE (RETWEET#>>'{user,id}')::bigint = 2753743832;
    SELECT * FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE USERID = 2753743832;
/********************************************************************************************AGGREGATION OF LOCATION FROM GEOTAGGED TWEETS AND UPDATE IN RETWEETS_API TABLE*********************************************************************************************************************/
ALTER TABLE RETWEETS_API ADD FIRST_RETWEET_XY GEOMETRY(POINT, 4326);
ALTER TABLE RETWEETS_API ADD TWEET_TYPE VARCHAR;
UPDATE RETWEETS_API SET TWEET_TYPE = 'SUPPORT' WHERE EXISTS (SELECT TWEET_ID FROM PARIS_ALL WHERE SUPPORT='T' AND TWEET_ID = RETWEETS_API.TWEET_ID);
UPDATE RETWEETS_API SET TWEET_TYPE = 'EVENTS' WHERE EXISTS (SELECT TWEET_ID FROM PARIS_ALL WHERE USEFUL='T' AND TWEET_ID = RETWEETS_API.TWEET_ID);
/******************************************************************************************** VERY IMPORTANT SET ALL GEOM COLUMNS TO NULL BEFORE UPDATING THEM******************************************************************************************************************************************/
--UPDATE RETWEETS_API RA SET FIRST_RETWEET_XY = NULL; UPDATE RETWEETS_API RA SET SECOND_RETWEET_XY = NULL; UPDATE RETWEETS_API SET FIRST_RETWEET_LINE = NULL; UPDATE RETWEETS_API SET SECOND_RETWEET_LINE = NULL;

UPDATE RETWEETS_API RA SET FIRST_RETWEET_XY = ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326)
        								FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
									WHERE TWEET_TYPE is null AND USERID=(RA.RETWEET#>>'{user,id}')::bigint AND DEPTH = 1 
									AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1;
--ALTER TABLE RETWEETS_API ADD SECOND_RETWEET_XY GEOMETRY(POINT, 4326);
UPDATE RETWEETS_API RA SET SECOND_RETWEET_XY = ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326)
        								FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
									WHERE USERID=(RA.RETWEET#>>'{user,id}')::bigint AND DEPTH = 2 
									AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -2 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 2;
/********************************************************************************************EXTRACTION OF THE ORIGINAL TWEET LOCATION FROM RETWEETED_STATUS, PLACE ETC*********************************************************************************************************************/
--ALTER TABLE RETWEETS_API ADD FIRST_RETWEET_LINE GEOMETRY(LINESTRING, 4326);ALTER TABLE RETWEETS_API ADD SECOND_RETWEET_LINE GEOMETRY(LINESTRING, 4326);
UPDATE RETWEETS_API SET FIRST_RETWEET_LINE = ST_MAKELINE(ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(retweet#>>'{retweeted_status, coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(retweet#>>'{retweeted_status,place,bounding_box}')))),4326)
								     ,FIRST_RETWEET_XY)
								     WHERE TWEET_TYPE is null AND FIRST_RETWEET_XY IS NOT NULL;
UPDATE RETWEETS_API SET SECOND_RETWEET_LINE = ST_MAKELINE(FIRST_RETWEET_XY,SECOND_RETWEET_XY) WHERE SECOND_RETWEET_XY IS NOT NULL;

SELECT * INTO FIRST_RETWEET_SUPPORT FROM RETWEETS_API WHERE TWEET_TYPE = 'SUPPORT' AND FIRST_RETWEET_XY IS NOT NULL; --DROP TABLE FIRST_RETWEET_SUPPORT;
SELECT ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(retweet#>>'{retweeted_status, geo}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(retweet#>>'{retweeted_status,place,bounding_box}')))),4326)
         ,retweet#>>'{retweeted_status,place,name}', retweet#>>'{retweeted_status,user,screen_name}', *
FROM RETWEETS_API WHERE TWEET_ID = 665286037783109633;

--drop table FIRST_RETWEET_LINE;drop table second_RETWEET_LINE;SELECT FIRST_RETWEET_LINE::geometry(linestring, 4326) AS LINE INTO FIRST_RETWEET_LINE FROM RETWEETS_API WHERE FIRST_RETWEET_LINE IS NOT NULL;SELECT SECOND_RETWEET_LINE::geometry(linestring, 4326) AS LINE INTO SECOND_RETWEET_LINE FROM RETWEETS_API WHERE SECOND_RETWEET_LINE IS NOT NULL;


SELECT * 
FROM RETWEETS_API WHERE TWEET_ID = 665281047509262336;

SELECT COUNT(*) FROM NE_I WHERE  USERID = ANY(ARRAY(SELECT DISTINCT USER_ID FROM RETWEETS_API WHERE TWEET_ID = 665281047509262336));
SELECT * FROM GEO_TAGGED_TWEETS_BY_RETWEETERS WHERE USERID::BIGINT = 81646497;

/********************************************************************************************WIERD FIRST RETWEET LINE CHECK*********************************************************************************************************************/
wkt_geom	retweet_id	tweet_id	user_id	first_retweet_xy	second_retweet_xy	second_retweet_line
LINESTRING(48.87142990000000253 2.36808569999999996, -58.56417449999999292 -34.54465899999999579)	914	665267239948566528	1475054576	SRID=4326;POINT(-58.5641745 -34.544659)	
SELECT retweet#>>'{retweeted_status, geo}',retweet#>>'{retweeted_status,place,name}',ST_ASTEXT(ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(retweet#>>'{retweeted_status,place,bounding_box}')))),* 
FROM RETWEETS_API WHERE TWEET_ID = 665267239948566528 AND FIRST_RETWEET_LINE IS NOT NULL;

SELECT ST_ASTEXT(ST_GeomFromGeoJSON(tweet#>>'{geo}')),ST_ASTEXT(ST_point((tweet#>>'{geo,coordinates,1}')::NUMERIC, (tweet#>>'{geo,coordinates,0}')::NUMERIC))
FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
LIMIT 10;

select tweet#>>'{place,bounding_box}',tweet#>>'{place,name}',*
from GEO_TAGGED_TWEETS_BY_RETWEETERS where userid in (400213409) and extract(day from t_diff_from_retweet) > -1 and extract(day from t_diff_from_retweet) < 1;

select * from retweets_api  where retweet_id=1565;
tweet_id::bigint = 665284734675771392 and first_retweet_xy is not null;



SELECT ST_ASTEXT(ST_SRID(ST_POINT(200,200),4326))
SELECT ST_SETSRID(ST_POINT(2000,222222),4326)

SELECT * FROM PARIS_ALL WHERE SUPPORT = 't' LIMIT 10;
SELECT * FROM FOLLOWERS LIMIT 10;

SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id
             FROM PARIS_ALL A WHERE SUPPORT = 't' AND NOT EXISTS
             (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND RETWEETS > 0;

             SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id
             FROM PARIS_ALL A WHERE SUPPORT = 't' AND NOT EXISTS
             (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND NOT EXISTS
             (SELECT USER_ID FROM FOLLOWERS_OF_FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND RETWEETS > 0;

             select * from followers_of_followers order by 1 desc limit 1000;
             select * from followers order by 1 desc limit 100;
             delete from followers_of_followers where follower_id = 0;
/*********************************************************************************************************************SELECTING ONLY COMPLETE RETWEET LINES************************************************************************************************************************************************************************************************************/
select * INTO FIRST_RETWEETS from retweets_api where first_retweet_xy is not null and first_retweet_line is not null;

select * INTO SECOND_RETWEETS from retweets_api where second_retweet_xy is not null and second_retweet_line is not null;


select count(distinct tweet_id)  from HASHTAGS_PARIS_ALL;

SELECT TWEET_ID::BIGINT,J#>>'{text}' AS HASHTAG
INTO HASHTAGS_PARIS_ALL 
FROM (SELECT TWEET#>>'{id}' AS TWEET_ID, jsonb_array_elements(TWEET#>'{entities,hashtags}') as J
	FROM PARIS_ALL 
	WHERE jsonb_array_length(TWEET#>'{entities,hashtags}') > 0) AS A;
ALTER TABLE HASHTAGS_PARIS_ALL ADD ID SERIAL;

SELECT COUNT(*) FROM PARIS_ALL;
SELECT * FROM HASHTAGS_PARIS_ALL LIMIT 10;
SELECT HASHTAG,COUNT(DISTINCT TWEET_ID) FROM HASHTAGS_PARIS_ALL GROUP BY HASHTAG ORDER BY 2 DESC LIMIT 100;
SELECT INSERT_TIME FROM PARIS_ALL ORDER BY 1 DESC LIMIT 10;

SELECT COUNT(*) FROM PARIS_ALL WHERE USEFUL = 't' AND EXISTS (SELECT * FROM RETWEETS_API RA WHERE RA.TWEET_ID = PARIS_ALL.TWEET_ID);

SELECT * FROM PARIS_ALL WHERE USEFUL='t' AND RETWEETS IS NULL;
SELECT * FROM RETWEETS_API ORDER BY 1 DESC LIMIT 10; --MAX ID =1807;
SELECT TWEET_ID, RETWEETS FROM PARIS_ALL A WHERE SUPPORT = 't' AND TWEET_ID != 665320468963008512 AND NOT EXISTS(SELECT TWEET_ID FROM RETWEETS_API WHERE TWEET_ID = A.TWEET_ID) AND RETWEETS>0;

SELECT EXTRACT(MONTH FROM INSERT_TIME), EXTRACT(DAY FROM INSERT_TIME), EXTRACT(HOUR FROM INSERT_TIME), HASHTAG,COUNT(DISTINCT HPA.TWEET_ID) 
FROM HASHTAGS_PARIS_ALL HPA, PARIS_ALL PA
WHERE HPA.TWEET_ID=PA.TWEET_ID
GROUP BY EXTRACT(MONTH FROM INSERT_TIME), EXTRACT(DAY FROM INSERT_TIME),EXTRACT(HOUR FROM INSERT_TIME),HASHTAG 
HAVING COUNT(DISTINCT HPA.TWEET_ID)  > 5
ORDER BY 1,2,3 ASC;

SELECT count(TWEET_ID) FROM PARIS_ALL WHERE RETWEETS IS NULL AND EXISTS(SELECT TWEET_ID FROM HASHTAGS_PARIS_ALL WHERE TWEET_ID=PARIS_ALL.TWEET_ID) ORDER BY 1LIMIT 10; 
SELECT TWEET_ID FROM PARIS_ALL WHERE RETWEETS IS NULL AND EXISTS(SELECT TWEET_ID FROM HASHTAGS_PARIS_ALL WHERE TWEET_ID=PARIS_ALL.TWEET_ID
             AND HASHTAG IN ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan')) ;

SELECT * FROM RETWEETS_API 
WHERE FIRST_RETWEET_XY IS NOT NULL AND SECOND_RETWEET_XY IS NOT NULL;

select count(distinct tweet_id) from retweets_api where tweet_id in (select tweet_id from paris_all where support = 't');
create index ix_paris_all_user_id on paris_all using btree(user_id);
create index ix_followers_user_id on followers using btree(user_id);
create index ix_GEO_TAGGED_TWEETS_BY_RETWEETERS_user_id on GEO_TAGGED_TWEETS_BY_RETWEETERS using btree(userid);

INSERT INTO GEO_TAGGED_TWEETS_BY_RETWEETERS (
SELECT 'SW',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 
FROM SW 
WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) = ANY(ARRAY(SELECT DISTINCT USER_ID FROM RETWEETS_API WHERE TWEET_ID IN (SELECT TWEET_ID FROM PARIS_ALL WHERE SUPPORT='t'))));
ALTER TABLE PARIS_ALL ADD TWEET_TYPE VARCHAR;
UPDATE PARIS_ALL SET TWEET_TYPE = CASE WHEN USEFUL='t' THEN 'EVENTS'
							WHEN SUPPORT='t' THEN 'SUPPORT'
							ELSE NULL END;



							SELECT 
							count(DISTINCT TWEET#>'{user,screen_name}')
							--DISTINCT TWEET#>'{user,screen_name}',TWEET#>'{user,followers_count}' 
             FROM PARIS_ALL A WHERE EXISTS (SELECT TWEET_ID FROM HASHTAGS_PARIS_ALL WHERE hashtag = 'ParisAttacks'
              ) AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT)
             AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS_OF_FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) 
             AND RETWEETS > 0 
             ORDER BY 2 DESC;

	     --drop table user_timeline;
             create table user_timeline(id serial, tweet jsonb);
             select * from user_timeline;
             select * from pg_stat_activity where datname = 'tgs7'
             select pg_cancel_backend(10176);


             select * from FOLLOWERS order by 1 desc limit 10;
             /******************************************************************************************************USER_ID IN PARIS_ALL******************************************************************************************************/
             alter table paris_all add user_id bigint;
             update paris_all set user_id = (TWEET#>>'{user,id}')::bigint;
             create index ix_paris_all_user_id on paris_all using btree(user_id);
             
             SELECT COUNT(DISTINCT TWEET_ID) FROM PARIS_ALL 
             WHERE --TWEET_TYPE IS NOT NULL AND 
             RETWEETS>0 AND 
             EXISTS(SELECT 1 FROM HASHTAGS_PARIS_ALL H WHERE hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan') AND H.TWEET_ID=PARIS_ALL.TWEET_ID)  AND
             EXISTS (SELECT 1 FROM RETWEETS_API WHERE TWEET_ID=PARIS_ALL.TWEET_ID);


             WITH CTE AS (SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id,(TWEET#>>'{user,followers_count}')::int as followers_count
              ,row_number() over(partition by  TWEET#>'{user,screen_name}',TWEET#>'{user,id}' order by (TWEET#>>'{user,followers_count}')::int asc)
             FROM PARIS_ALL A WHERE EXISTS (SELECT TWEET_ID FROM HASHTAGS_PARIS_ALL WHERE TWEET_ID = A.TWEET_ID AND hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan')
              ) AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT)
             AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS_OF_FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND RETWEETS > 0
             ORDER BY (TWEET#>>'{user,followers_count}')::int ASC)
             SELECT * FROM CTE WHERE ROW_NUMBER=1 AND FOLLOWERS_COUNT < 1000;

             SELECT COUNT(*) FROM FOLLOWERS

		
             SELECT COUNT(DISTINCT TWEET_ID) FROM RETWEETS_API WHERE EXISTS(SELECT TWEET_ID FROM HASHTAGS_PARIS_ALL 
														WHERE hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan')
														AND TWEET_ID=RETWEETS_API.TWEET_ID);


             INSERT INTO GEO_TAGGED_TWEETS_BY_RETWEETERS (SELECT 'SW',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 
		FROM SW 
		WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) = ANY(ARRAY(SELECT DISTINCT USER_ID FROM RETWEETS_API WHERE TWEET_ID IN (SELECT TWEET_ID FROM PARIS_ALL WHERE SUPPORT='t'))));

             INSERT INTO GEO_TAGGED_TWEETS_BY_RETWEETERS (SELECT 'NE_II',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 
		FROM NE_II 
		WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) = ANY(ARRAY(SELECT DISTINCT USER_ID FROM RETWEETS_API 
														WHERE TWEET_ID 
														IN (SELECT TWEET_ID 
													        FROM HASHTAGS_PARIS_ALL 
													        WHERE hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan'))))
													        AND ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) NOT IN 
													        (SELECT DISTINCT ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) FROM GEO_TAGGED_TWEETS_BY_RETWEETERS));
SELECT COUNT(*) FROM RETWEETS_API WHERE  
TWEET_ID IN (SELECT DISTINCT TWEET_ID FROM HASHTAGS_PARIS_ALL
WHERE TWEET_TYPE = 'EVENTS');
WHERE hashtag in ('prayforparis','PrayForParis','fusillade','SaintDenis','Bataclan')); 
WHERE hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan'));

		SELECT COUNT(DISTINCT USERID) 
		FROM GEO_TAGGED_TWEETS_BY_RETWEETERS G
	        WHERE EXISTS( SELECT 1 
				     FROM RETWEETS_API 
				     WHERE USER_ID = G.USERID AND EXISTS(SELECT TWEET_ID 
						                                           FROM HASHTAGS_PARIS_ALL 
						                                           WHERE hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan')
						                                           AND TWEET_ID=RETWEETS_API.TWEET_ID));
select * from retweets_api where tweet_type is null limit 10;
select * from retweets_api limit 10;
SELECT DISTINCT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF 
			  FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
                                                 WHERE A.TWEET_TYPE is null AND A.USER_ID=B.USERID
update retweets_api set user_id = (retweet#>>'{user,id}')::bigint;

CREATE INDEX IX_HASHTAGS_PARIS_ALL ON HASHTAGS_PARIS_ALL USING BTREE(TWEET_ID);

SELECT HASHTAG,ROUND(AVG(FAVOURITES),2), ROUND(AVG(RETWEETS),2), COUNT(DISTINCT HP.TWEET_ID) 
FROM HASHTAGS_PARIS_ALL HP,PARIS_ALL_NO_TWEETS P WHERE HP.TWEET_ID=P.TWEET_ID
--AND USER_ID IN (SELECT DISTINCT USER_ID FROM FOLLOWERS)
GROUP BY HASHTAG ORDER BY 4 DESC  LIMIT 50;

update PARIS_ALL set user_id = (TWEET#>>'{user,id}')::bigint;
alter table PARIS_ALL ADD JOURNALIST BOOLEAN;

with cte as(SELECT TWEET#>>'{user,screen_name}' AS SCREEN_NAME, TWEET#>>'{user,description}' AS DESCRIPTION,row_number() over(partition by TWEET#>>'{user,screen_name}') as rn FROM PARIS_ALL WHERE TWEET_TYPE='EVENTS')
SELECT * FROM CTE WHERE RN = 1 AND DESCRIPTION NOT ILIKE '%journ%' AND NOT LIKE '%CNN%' AND NOT ILIKE '%REPORTER%';

UPDATE PARIS_ALL SET JOURNALIST = 'True' WHERE TWEET_TYPE IS NOT NULL AND TWEET#>>'{user,description}' ILIKE '%REPORTER%';
UPDATE PARIS_ALL SET JOURNALIST = 'True' WHERE TWEET_TYPE IS NOT NULL AND TWEET#>>'{user,description}' ILIKE '%journ%';
UPDATE PARIS_ALL SET JOURNALIST = 'True' WHERE TWEET_TYPE IS NOT NULL AND TWEET#>>'{user,description}' ILIKE '%news%';
SELECT * FROM PARIS_ALL WHERE INSERT_TIME > '2015-11-13 20:15:00.000000' AND INSERT_TIME < '2015-11-14 01:00:00.000000' LIMIT 10;

SELECT * FROM PARIS_ALL WHERE TWEET_TYPE = 'EVENTS' ORDER BY INSERT_TIME DESC LIMIT 10;


SELECT TWEET#>>'{text}' AS text
         ,(TWEET#>>'{favorited}')::boolean as favorited
         ,tweet#>>'{in_reply_to_screen_name}' as replyToSN
         ,(tweet#>>'{created_at}')::timestamp as created
         ,(tweet#>>'{truncated}')::boolean as truncated
          ,(tweet#>>'{in_reply_to_status_id}')::bigint as inReplyToSID
          ,(tweet#>>'{id}')::bigint as id
          ,(tweet#>>'{in_reply_to_user_id}')::bigint as inReplyToUID
          ,substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) as statusSource
          ,(tweet#>>'{user,screen_name}') as screenName  
FROM PARIS_ALL WHERE INSERT_TIME > '2015-11-13 22:00:00.000000' AND INSERT_TIME < '2015-11-13 23:00:00.000000' LIMIT 10;


SELECT COUNT(*) FROM PARIS_ALL WHERE INSERT_TIME > '2015-11-13 21:00:00.000000' AND INSERT_TIME < '2015-11-14 08:00:00.000000' AND TWEET#>>'{lang}'='en'
and (
	TWEET#>>'{text}' ilike '% safe %' OR TWEET#>>'{text}' ilike '% pray%' OR TWEET#>>'{text}' ilike '% tonight %' OR TWEET#>>'{text}' ilike '% thoughts %' OR TWEET#>>'{text}' ilike '% attack%' OR
	TWEET#>>'{text}' ilike '% news %' OR TWEET#>>'{text}' ilike '% peace %' OR	TWEET#>>'{text}' ilike '% dead %' OR	TWEET#>>'{text}' ilike '% police %' OR	TWEET#>>'{text}' ilike '% happen%' OR
	TWEET#>>'{text}' ilike '% victim%' OR TWEET#>>'{text}' ilike '% bataclan %' OR TWEET#>>'{text}' ilike '% terror%' OR TWEET#>>'{text}' ilike '% horrible %' OR TWEET#>>'{text}' ilike '% tragedy %' OR
	TWEET#>>'{text}' ilike '% scared %' OR
	TWEET#>>'{text}' ilike '% stay % strong %'
	); 
	/*OR	TWEET#>>'{text}' ilike '% tonight %' OR TWEET#>>'{text}' ilike '% safe %' OR TWEET#>>'{text}' ilike '% Bataclan %' OR TWEET#>>'{text}' ilike '% photo %' OR
	TWEET#>>'{text}' ilike '% live %' OR TWEET#>>'{text}' ilike '% heart %' OR TWEET#>>'{text}' ilike '% attack %' OR TWEET#>>'{text}' ilike '% prayers %' OR TWEET#>>'{text}' ilike '% home %' OR
	TWEET#>>'{text}' ilike '% stay %' OR	TWEET#>>'{text}' ilike '% friends %' OR TWEET#>>'{text}' ilike '% thoughts %' OR TWEET#>>'{text}' ilike '% dead %' OR TWEET#>>'{text}' ilike '% peace %' OR
	TWEET#>>'{text}' ilike '% everyone %' OR	TWEET#>>'{text}' ilike '% Praying %' OR TWEET#>>'{text}' ilike '% news %' OR TWEET#>>'{text}' ilike '% today %' OR	TWEET#>>'{text}' ilike '% place %' OR
	TWEET#>>'{text}' ilike '% know %');*/

SELECT count(id) FROM PARIS_ALL WHERE INSERT_TIME > '2015-11-13 21:00:00.000000' AND INSERT_TIME < '2015-11-14 08:00:00.000000' AND TWEET#>>'{lang}'='fr'
and (
	TWEET#>>'{text}' ilike '% familles %' OR TWEET#>>'{text}' ilike '% peux  %' OR TWEET#>>'{text}' ilike '% peur %' OR TWEET#>>'{text}' ilike '% fin %' OR TWEET#>>'{text}' ilike '% terroristes %' OR
	TWEET#>>'{text}' ilike '% mort %' OR TWEET#>>'{text}' ilike '% terroriste %' OR TWEET#>>'{text}' ilike '% attentat %' OR TWEET#>>'{text}' ilike '% victimes %' OR	TWEET#>>'{text}' ilike '% horrible %' OR
	TWEET#>>'{text}' ilike '% sécurité  %' OR TWEET#>>'{text}' ilike '% guerre %' OR TWEET#>>'{text}' ilike '% fou %' OR TWEET#>>'{text}' ilike '% police %' OR TWEET#>>'{text}' ilike '% attaques %' OR
	TWEET#>>'{text}' ilike '% tuer %' OR TWEET#>>'{text}' ilike '% fusillades %' OR TWEET#>>'{text}' ilike '% musulmans %' OR TWEET#>>'{text}' ilike '% sérieux %' OR TWEET#>>'{text}' ilike '% blessés %' OR
	TWEET#>>'{text}' ilike '% Bataclan %' OR TWEET#>>'{text}' ilike '% horreur %' OR TWEET#>>'{text}' ilike '% mal %' OR TWEET#>>'{text}' ilike '% morts %' OR TWEET#>>'{text}' ilike '% terrorisme %' OR
	TWEET#>>'{text}' ilike '% Fusillade %' OR	TWEET#>>'{text}' ilike '% frontières %' OR TWEET#>>'{text}' ilike '% attentats %' OR TWEET#>>'{text}' ilike '% urgence %' OR TWEET#>>'{text}' ilike '% pire %' OR
	TWEET#>>'{text}' ilike '% stade %' OR TWEET#>>'{text}' ilike '% Praying %' OR TWEET#>>'{text}' ilike '% news %' OR TWEET#>>'{text}' ilike '% today %' OR	TWEET#>>'{text}' ilike '% place %' OR
	TWEET#>>'{text}' ilike '% know %');


	SELECT * FROM PARIS_ALL where freq_words = true LIMIT 10;
	ALTER TABLE PARIS_ALL ADD FREQ_WORDS BOOLEAN;
	UPDATE PARIS_ALL SET FREQ_WORDS = TRUE WHERE INSERT_TIME > '2015-11-13 21:00:00.000000' AND INSERT_TIME < '2015-11-14 08:00:00.000000' AND TWEET#>>'{lang}'='en'
	and (
		TWEET#>>'{text}' ilike '% safe %' OR TWEET#>>'{text}' ilike '% pray%' OR TWEET#>>'{text}' ilike '% tonight %' OR TWEET#>>'{text}' ilike '% thoughts %' OR TWEET#>>'{text}' ilike '% attack%' OR
		TWEET#>>'{text}' ilike '% news %' OR TWEET#>>'{text}' ilike '% peace %' OR	TWEET#>>'{text}' ilike '% dead %' OR	TWEET#>>'{text}' ilike '% police %' OR	TWEET#>>'{text}' ilike '% happen%' OR
		TWEET#>>'{text}' ilike '% victim%' OR TWEET#>>'{text}' ilike '% bataclan %' OR TWEET#>>'{text}' ilike '% terror%' OR TWEET#>>'{text}' ilike '% horrible %' OR TWEET#>>'{text}' ilike '% tragedy %' OR
		TWEET#>>'{text}' ilike '% scared %' OR
		TWEET#>>'{text}' ilike '% stay % strong %'
		); 

SELECT count(*) FROM PARIS_ALL where freq_words = true and retweets is null;
SELECT 'https://www.twitter.com/statuses/'||tweet_id,ID FROM PARIS_ALL WHERE freq_words = true AND RETWEETS IS NULL ORDER BY 1 LIMIT 10;

SELECT TWEET#>>'{entities,media,0,expanded_url}',ID FROM PARIS_ALL WHERE SUPPORT='t' and retweets > 0 ORDER BY 1 limit 10;

WITH CTE AS (SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id,(TWEET#>>'{user,followers_count}')::int as followers_count
           ,row_number() over(partition by  TWEET#>'{user,screen_name}',TWEET#>'{user,id}' order by (TWEET#>>'{user,followers_count}')::int asc)--, retweets
             FROM PARIS_ALL A WHERE freq_words = TRUE AND RETWEETS > 0
             AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT)
             AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS_OF_FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) 
             ORDER BY (TWEET#>>'{user,followers_count}')::int ASC)
             SELECT * FROM CTE WHERE ROW_NUMBER=1 ;
             AND FOLLOWERS_COUNT <= 50;

             select * from followers order by 1 desc limit 100;  --43,578,145
             create index ix_followers_id on followers using btree(id);

select tweet_type, retweets, count(tweet_id) 
from PARIS_ALL 
where freq_words = TRUE 
group by retweets,tweet_type
order by count(tweet_id) desc;


select * from PARIS_ALL where freq_words = TRUE and retweets is not null limit 50;
select * from PARIS_ALL where freq_words = TRUE and retweets > 0 limit 50;

select tweet_type, count(distinct tweet_id) 
from paris_all a, followers f
where a.user_id=f.user_id
group by tweet_type

select count(distinct tweet_id) from paris_all a where freq_words = TRUE and retweets > 0 and tweet_type is null
--and not exists (select 1 from paris_all where user_id =a.user_id and tweet_type is not null)
and not exists (select 1 from hashtags where tweet_id = a.tweet_id)

select count(distinct tweet_id) from paris_all a where freq_words = TRUE and retweets > 0 --and tweet_type is null
and not exists (select 1 from retweets_api where tweet_id = a.tweet_id)
and not exists (select 1 from retweets_api_new where tweet_id = a.tweet_id);

select count(distinct tweet_id) from paris_all a where freq_words = TRUE and retweets > 0 and tweet_type is null
and not exists (select 1 from hashtags where tweet_id = a.tweet_id)

alter table paris_all add hash_flag boolean;
update paris_all a set hash_flag = true where exists (select 1 from hashtags where tweet_id=a.tweet_id);
SELECT * FROM PARIS_ALL where hash_flag = true limit 10;

select tweet_id, tweet#>>'{text}' from paris_all where tweet#>>'{text}' ilike '%RT @%' limit 20;
select twe from paris_all where tweet_id = 666921931262832640;
--for igraph
select tweet#>>'{user,screen_name}'  as who_posted,retweet#>>'{user,screen_name}' as who_retweeted from 
paris_all p, retweets_api r
where p.tweet_id=r.tweet_id;


select * from retweets_api WHERE FIRST_RETWEET_XY IS NOT NULL limit 10;

SELECT DISTINCT TWEET_ID FROM PARIS_ALL A WHERE A.FREQ_WORDS IS NOT NULL AND A.RETWEETS > 0 AND NOT EXISTS (SELECT 1 FROM RETWEETS_API WHERE TWEET_ID = A.TWEET_ID); --13 left
SELECT COUNT(DISTINCT TWEET_ID) FROM PARIS_ALL WHERE FREQ_WORDS IS NOT NULL; --2854
SELECT COUNT(DISTINCT TWEET_ID) FROM PARIS_ALL WHERE FREQ_WORDS IS NOT NULL AND RETWEETS >0; -- > 0 - 510, =0 - 2344, 

SELECT DISTINCT TWEET_ID FROM PARIS_ALL A WHERE A.FREQ_WORDS IS NOT NULL AND A.RETWEETS > 0 AND NOT EXISTS (SELECT 1 FROM RETWEETS_API WHERE TWEET_ID = A.TWEET_ID);
select count(distinct user_id) from followers; --3339

set enable_seqscan = false;
select freq_words,hash_flag,tweet_type, count(distinct a.user_id)
from paris_all a,followers b
where --(tweet_type is not null or hash_flag is not null or tweet_type is not null) and 
a.user_id=b.user_id
group by freq_words,hash_flag,tweet_type
order by count(distinct a.user_id) desc;
--freq_words;hash_flag;tweet_type;count
--<NULL>;t;<NULL>;3143
--<NULL>;<NULL>;<NULL>;2337
--t;<NULL>;<NULL>;289
--t;t;<NULL>;224
--<NULL>;t;SUPPORT;116
--<NULL>;t;EVENTS;87
--<NULL>;<NULL>;SUPPORT;69
--<NULL>;<NULL>;EVENTS;67
--t;t;EVENTS;12
--t;<NULL>;EVENTS;7
--t;<NULL>;SUPPORT;4
--t;t;SUPPORT;3

set enable_seqscan = false;
update paris_all a set hash_flag = TRUE where exists (select 1 from hashtags_paris_all where tweet_id = a.tweet_id); --Query returned successfully: 174,402 rows affected, 98,250 ms execution time.

select * from followers order by 1 desc limit 10;
select tweet#>>'{user,screen_name}' from paris_all where user_id = 154011677;
select count(follower_id) from followers where user_id = 154011677; --989841
select count(distinct follower_id) from followers where user_id = 154011677; ---79842 correct # of followers, no duplicates
--followers stats once and for all...
set enable_seqscan = false;
SELECT TWEET#>>'{user,screen_name}' FROM PARIS_ALL A WHERE A.FREQ_WORDS IS NOT NULL AND A.RETWEETS > 0 
AND NOT EXISTS (SELECT 1 FROM followers WHERE user_id = A.user_id)
AND NOT EXISTS (SELECT 1 FROM followers_of_followers WHERE user_id = A.user_id); --only 5

---FOLLOWERS AMONG RETWEETERS
--neccessary maintenance:
SELECT * FROM RETWEETS_API LIMIT 10;
UPDATE RETWEETS_API SET USER_ID = (RETWEET#>>'{user,id}')::bigint;
reindex INDEX ix_retweets_api_user_id;
create index ix_followers_follower_id on followers using btree(follower_id);

set enable_seqscan = false;
SELECT 
count(distinct retweet#>>'{id}') 
--count(distinct tweet_id)
FROM RETWEETS_API a
where 
exists (SELECT 1 FROM PARIS_ALL WHERE hash_flag IS NOT NULL AND RETWEETS > 0 and tweet_id = a.tweet_id)  --5463 distinct tweets, 19959 distinct retweet
--exists (SELECT 1 FROM PARIS_ALL WHERE tweet_type IS NOT NULL AND RETWEETS > 0 and tweet_id = a.tweet_id)  --359 distinct tweets, 1640 distinct retweets
--exists (SELECT 1 FROM PARIS_ALL WHERE FREQ_WORDS IS NOT NULL AND RETWEETS > 0 and tweet_id = a.tweet_id)  --441 distinct tweets have frequent words and have been retweeted by a follower (1198 distinct retweets)
and exists (select 1 from followers where follower_id = a.user_id)
;

select count(*) from GEO_TAGGED_TWEETS_BY_RETWEETERS;
INSERT INTO GEO_TAGGED_TWEETS_BY_RETWEETERS (SELECT 'SE',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,2 FROM SE 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS_OF_FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));

	  SELECT DISTINCT RA.USER_ID 
	  FROM RETWEETS_API RA
	  where exists (select 1 from followers where ra.user_id=follower_id);

select count(distinct retweet#>>'{id}') from retweets_api ra
where exists (select 1 from paris_all where tweet_id = ra.tweet_id and (tweet_type is not null or freq_words is not null or hash_flag is not null)); --retweets_api has retweets of only useful tweets...31322

SELECT count(distinct retweet#>>'{id}') FROM RETWEETS_API a where exists (select 1 from followers where follower_id = a.user_id); --only 21159 were retweeted by the followers;

CREATE TABLE geo_tweets_by_retweeters_complete
(
  tablename character varying,
  tableid integer,
  tweetid text,
  userid bigint,
  tweet jsonb,
  t_diff_from_retweet interval,
  depth integer
);
set enable_seqscan = false;
--update retweets_api set user_id = (retweet#>>'{user,id}')::bigint;
select * from geo_tweets_by_retweeters_complete limit 10;
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'NE_I',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM NE_I 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'NE_II',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM NE_II 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'NW_I',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM NW_I 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'NW_II',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM NW_II 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'NW_III',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM NW_III 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'SE',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM SE 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
INSERT INTO geo_tweets_by_retweeters_complete (SELECT 'SW',ID,(TWEET#>>'{id}')::BIGINT,((((tweet -> 'user'::text) ->> 'id'::text)::bigint)),TWEET,NULL,1 FROM SW 
									  WHERE ((((tweet -> 'user'::text) ->> 'id'::text)::bigint)) 
									  = ANY(ARRAY(SELECT DISTINCT RA.USER_ID FROM RETWEETS_API RA, FOLLOWERS FOF WHERE RA.USER_ID=FOF.FOLLOWER_ID)));
update geo_tweets_by_retweeters_complete set depth = 1; --since retweeters are in followers table not followers of followers
update geo_tweets_by_retweeters_complete set depth = 2 where user_id in (select distinct follower_id from followers_of_followers) and depth is null; --3.122 rows affected
update geo_tweets_by_retweeters_complete set depth = 3 where depth is null and user_id not in (select distinct follower_id from followers_of_followers)  
										  and user_id not in (select distinct follower_id from followers); --52471 rows affected, 01:45 minutes execution time.
SELECT COUNT(DISTINCT RETWEET_ID) FROM RETWEETS_API WHERE next_tweet_location IS NULL;--20.682
SELECT COUNT(DISTINCT RETWEET_ID) FROM RETWEETS_API WHERE next_tweet_location IS not NULL;--1.561

select count(distinct user_id) from geo_tweets_by_retweeters_complete where user_id in (select user_id from followers);--181
select count(distinct user_id) from geo_tweets_by_retweeters_complete where user_id in (select follower_id from followers);--2.171
select count(distinct user_id) from geo_tweets_by_retweeters_complete where user_id in (select follower_id from followers_of_followers);--337

with cte as (select distinct user_id from paris_all where retweets > 0)
select count(distinct user_id) from retweets_api where 
	user_id in (select follower_id from followers_of_followers) and 
	user_id in (select distinct user_id from cte) and 
	user_id in (select distinct user_id from geo_tweets_by_retweeters_complete);--46

with cte as (select distinct user_id from paris_all where retweets > 0)
select count(distinct user_id) from retweets_api where 
	user_id in (select follower_id from followers) and 
	user_id in (select distinct user_id from cte) and 
	user_id in (select distinct user_id from geo_tweets_by_retweeters_complete);--193
	select count(distinct user_id) from followers_of_followers;--221

select * into geo_tweets_by_retweeters_complete_backup from geo_tweets_by_retweeters_complete;
select * from geo_tweets_by_retweeters_complete limit 10;
alter table geo_tweets_by_retweeters_complete rename tweetid to tweet_id;
alter table geo_tweets_by_retweeters_complete rename userid to user_id;
alter table geo_tweets_by_retweeters_complete ADD GEOM_POINT GEOMETRY(POINT, 4326);
alter table geo_tweets_by_retweeters_complete ADD GEOM_POLYGON GEOMETRY(POLYGON, 4326);
select tweet#>'{coordinates}',tweet#>>'{place,bounding_box}' from geo_tweets_by_retweeters_complete where tweet?'cooridnates' = 't' limit 100;
UPDATE geo_tweets_by_retweeters_complete SET GEOM_POINT = st_setsrid(ST_GeomFromGeoJSON(TWEET#>>'{coordinates}'),4326) where (TWEET#>'{geo}')!='null';
UPDATE geo_tweets_by_retweeters_complete SET GEOM_POLYGON = st_setsrid(ST_GeomFromGeoJSON(TWEET#>>'{place,bounding_box}'),4326) where (TWEET#>'{geo}')='null';
--alter table geo_tweets_by_retweeters_complete add created_at timestamp;
update geo_tweets_by_retweeters_complete set created_at = (tweet#>>'{created_at}')::timestamp;
alter table retweets_api add created_at timestamp;
update retweets_api set created_at = (retweet#>>'{created_at}')::timestamp;
select * from retweets_api limit 10;

select count(distinct user_id) from retweets_api;-18.434
set enable_seqscan=false;
select count(distinct ra.user_id) from retweets_api ra, followers f where ra.user_id=f.follower_id;--12.731
create index ix_fof_user_id on followers_of_followers using btree(user_id);
create index ix_fof_follower_id on followers_of_followers using btree(follower_id);
set enable_seqscan=false;
select count(distinct ra.user_id) from retweets_api ra, followers_of_followers f where ra.user_id=f.follower_id;--1.876



UPDATE GEO_TAGGED_TWEETS_BY_RETWEETERS SET T_DIFF_FROM_RETWEET = SUB.TIME_DIFF 
		FROM (SELECT DISTINCT B.TWEETID,((TWEET#>>'{created_at}')::timestamp - (RETWEET#>>'{created_at}')::timestamp) AS TIME_DIFF 
			  FROM RETWEETS_API A, GEO_TAGGED_TWEETS_BY_RETWEETERS B
                                                 WHERE A.TWEET_TYPE is null AND A.USER_ID=B.USERID ) AS SUB
                                                 WHERE  GEO_TAGGED_TWEETS_BY_RETWEETERS.TWEETID = SUB.TWEETID AND GEO_TAGGED_TWEETS_BY_RETWEETERS.DEPTH=1;
                                                 
select st_astext(ST_GeomFromGeoJSON(TWEET#>>'{place,bounding_box}')),TWEET#>>'{place,bounding_box}'
from ne_i  where (TWEET#>'{geo}')='null' limit 10;

select * from geo_tweets_by_retweeters_complete where T_DIFF_FROM_RETWEET is not null limit 10
;
select user_id,count(distinct coalesce(geom_point,geom_polygon)) 
from geo_tweets_by_retweeters_complete 
group by user_id 
--having count(distinct coalesce(geom_point,geom_polygon)) > 1
order by 2 desc;

---distribution of retweets in time:
select extract(year from created_at) as year,extract(month from created_at) as month,extract(day from created_at) as day,count(*) 
from retweets_api
group by extract(year from created_at),extract(month from created_at),extract(day from created_at)
order by 1,2,3 asc;

----retweets_api centriod of recent geo tweets
create index ix_geo_tweets_by_retweeters_complete_user_id on geo_tweets_by_retweeters_complete using btree(user_id);

set enable_seqscan = false;
select a.retweet_id, b.tweet_id, extract(DAY from b.created_at - a.created_at),b.created_at - a.created_at, st_astext(coalesce(geom_point,st_centroid(st_makevalid(geom_polygon))))
from retweets_api a, geo_tweets_by_retweeters_complete b
where a.user_id = 211942334 and a.user_id=b.user_id 
order by 1,2 asc;
--26350: POINT(2.4912635 48.9458)
--31091: POINT(2.4912635 48.9458)

alter table retweets_api add next_tweet_location geometry(point, 4326);
alter table retweets_api rename retweet_id to id;
alter table retweets_api add retweet_id bigint;
update retweets_api set retweet_id = (retweet#>>'{id}')::bigint;
alter table geo_tweets_by_retweeters_complete alter column tweet_id type bigint using tweet_id::bigint;
create index ix_geo_tweets_by_retweeters_complete_tweet_id on geo_tweets_by_retweeters_complete using btree(tweet_id);

--FIXING RETWEET LOCATION: 12 HOURS ONLY AFTER AND BEFORE THE RETWEET
UPDATE retweets_api set first_RETWEET_XY = null;
update retweets_api p set first_RETWEET_XY = t.geom from (
									select distinct a.retweet_id, b.tweet_id,coalesce(geom_point,st_centroid(st_makevalid(geom_polygon))) as geom, a.user_id as user_id
									,b.created_at-a.created_at as real_interval
									,row_number() over (partition by a.user_id,a.retweet_id order by a.user_id,a.retweet_id, abs(extract(epoch from (b.created_at-a.created_at))) asc)
									from retweets_api a, geo_tweets_by_retweeters_complete b
									where  a.user_id=b.user_id and b.depth =1 --and a.user_id = 211942334
									and abs(extract(epoch from (b.created_at-a.created_at))) <21600  --< 43200
									order by 1,2 asc
									) as t
									where p.retweet_id=t.retweet_id and row_number=1;
ALTER TABLE RETWEETS_API RENAME NEXT_TWEET_LOCATION TO FIRST_RETWEET_XY;
ALTER TABLE RETWEETS_API ADD SECOND_RETWEET_XY geometry(Point,4326);
ALTER TABLE RETWEETS_API ADD NTH_RETWEET_XY geometry(Point,4326);

UPDATE retweets_api set SECOND_RETWEET_XY = null;
update retweets_api p set SECOND_RETWEET_XY = t.geom from (
									select distinct a.retweet_id, b.tweet_id,coalesce(geom_point,st_centroid(st_makevalid(geom_polygon))) as geom, a.user_id as user_id
									,b.created_at-a.created_at as real_interval
									,row_number() over (partition by a.user_id,a.retweet_id order by a.user_id,a.retweet_id, abs(extract(epoch from (b.created_at-a.created_at))) asc)
									from retweets_api a, geo_tweets_by_retweeters_complete b
									where  a.user_id=b.user_id and depth =2 --and a.user_id = 211942334
									--and abs(extract(epoch from (b.created_at-a.created_at))) <172800 --< 43200
									order by 1,2 asc
									) as t
									where p.retweet_id=t.retweet_id and row_number=1; --21

update retweets_api p set NTH_RETWEET_XY = t.geom from (
									select distinct a.retweet_id, b.tweet_id,coalesce(geom_point,st_centroid(st_makevalid(geom_polygon))) as geom, a.user_id as user_id
									,b.created_at-a.created_at as real_interval
									,row_number() over (partition by a.user_id,a.retweet_id order by a.user_id,a.retweet_id, abs(extract(epoch from (b.created_at-a.created_at))) asc)
									from retweets_api a, geo_tweets_by_retweeters_complete b
									where  a.user_id=b.user_id and depth =1 and a.user_id = 211942334
									--and abs(extract(epoch from (b.created_at-a.created_at))) <172800 --< 43200
									order by 1,2 asc
									) as t
									where p.retweet_id=t.retweet_id and row_number=1; --759
update retweets_api set first_retweet_line = NULL;
update retweets_api set first_retweet_line = st_makeline(original_tweet_xy,FIRST_RETWEET_XY);
update retweets_api set SECOND_RETWEET_LINE = NULL;
update retweets_api set SECOND_RETWEET_LINE = st_makeline(FIRST_RETWEET_XY,SECOND_RETWEET_XY);
alter table retweets_api add nth_retweet_line geometry(LineString,4326);
update retweets_api set nth_retweet_line = NULL;
update retweets_api set nth_retweet_line = st_makeline(original_TWEET_XY,nth_RETWEET_XY);									
select * from retweets_api where SECOND_RETWEET_XY is not null;
									
select extract(epoch from interval '12 hours');
select retweet_id,st_astext(next_tweet_location) from retweets_api where user_id = 211942334;
665338676709298178;POINT(2.4912635 48.9458)
665321505048408064;POINT(2.4912635 48.9458)

select * from retweets_api where user_id = 211942334;
----WRONG
UPDATE RETWEETS_API RA SET FIRST_RETWEET_XY = ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326)
        								FROM GEO_TAGGED_TWEETS_BY_RETWEETERS 
									WHERE TWEET_TYPE is not null AND USERID=(RA.RETWEET#>>'{user,id}')::bigint AND DEPTH = 1 
									AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) > -1 AND EXTRACT(DAY FROM T_DIFF_FROM_RETWEET) < 1;
-----CLEANING UP DUPLICATES FROM RETWEETS_API
with cte as (
select id,retweet_id, row_number() over(partition by retweet_id order by id asc ) as rn from retweets_api order by 1 asc)
delete from retweets_api where id in (select id from cte where rn > 1);
SELECT * FROM RETWEETS_API LIMIT 10;
select count(*) from geo_tagged_tweets_by_retweeters;
alter table retweets_api rename first_retweet_xy to original_tweet_xy;
update retweets_api set original_tweet_xy = NULL;
update retweets_api a set original_tweet_xy = place_xy from paris_all b where b.tweet_id = a.tweet_id;

select * from retweets_api limit 10;
select * from paris_all where tweet_id = 665358833762570240;


---summary by flags for paris_all
select date_trunc('day',insert_time)::date, tweet_type, hash_flag, freq_words, count(*) 
from paris_all 
group by date_trunc('day',insert_time)::date, tweet_type, hash_flag, freq_words
order by 1 asc;

select * from retweets_api where first_retweet_line is not null limit 10;
--summary for retweets_api:
select date_trunc('day',insert_time)::date, a.tweet_type, a.hash_flag, a.freq_words, count(distinct tweet_id) as n_of_tweets,count(distinct retweet_id) as n_of_retweets,sum(retweets) as total_retweets, round(avg(retweets),1) as avg_retweets
from paris_all a, retweets_api b 
where a.tweet_id=b.tweet_id and next_tweet_location is not null
group by date_trunc('day',insert_time)::date, a.tweet_type, a.hash_flag, a.freq_words
order by 1 asc;

--summary for paris_all:
select date_trunc('day',insert_time)::date, a.tweet_type, a.hash_flag, a.freq_words, count(*),sum(retweets) as total_retweets, round(avg(retweets),1) as avg_retweets
from paris_all a--, retweets_api b 
--where a.tweet_id=b.tweet_id and next_tweet_location is not null
group by date_trunc('day',insert_time)::date, a.tweet_type, a.hash_flag, a.freq_words
order by 1 asc;

ALTER TABLE RETWEETS_API ADD ORIGINAL_TWEET_DATE TIMESTAMP;
ALTER TABLE RETWEETS_API ADD HASH_FLAG BOOLEAN;
ALTER TABLE RETWEETS_API ADD FREQ_WORDS BOOLEAN;
alter table retweets_api add retweeter_screen_name varchar;
------JOURNALIST EXPLORATION....
alter table retweets_api add retweeter_desc varchar;
alter table retweets_api add source varchar;

update retweets_api set ORIGINAL_TWEET_DATE = NULL;update retweets_api A set ORIGINAL_TWEET_DATE = INSERT_TIME FROM PARIS_ALL B WHERE B.TWEET_ID = A.TWEET_ID;
update retweets_api set HASH_FLAG = NULL;update retweets_api A set HASH_FLAG = B.HASH_FLAG FROM PARIS_ALL B WHERE B.TWEET_ID = A.TWEET_ID;
update retweets_api set FREQ_WORDS = NULL;update retweets_api A set FREQ_WORDS = B.FREQ_WORDS FROM PARIS_ALL B WHERE B.TWEET_ID = A.TWEET_ID;
update retweets_api set retweeter_screen_name = NULL;update retweets_api set retweeter_screen_name = retweet#>>'{user,screen_name}';
update retweets_api set retweeter_desc = NULL;update retweets_api set retweeter_desc = retweet#>>'{user,description}';
update retweets_api set source = NULL;update retweets_api set source = retweet#>>'{user,description}';
update retweets_api set source = NULL;update retweets_api set source = substring(RETWEET#>>'{source}',position('>' in RETWEET#>>'{source}')+1,position('</' in RETWEET#>>'{source}')-position('>' in RETWEET#>>'{source}')-1);
select substring(RETWEET#>>'{source}',position('>' in RETWEET#>>'{source}')+1,position('</' in RETWEET#>>'{source}')-position('>' in RETWEET#>>'{source}')-1) from retweets_api limit 10;

-----WIERD CASE... XY POINT TO PARIS SUBURBS BUT QGIS PLOTS IT IN THE MIDDLE OF FRANCE
select count(*) from retweets_api where next_tweet_location is not null and first_retweet_line is not null;
select *,st_astext(original_tweet_xy) from retweets_api where next_tweet_location is not null and retweeter_screen_name = 'vdemaup'; ---POINT(2.4685685 48.8198275)
select *,st_astext(original_tweet_xy) from retweets_api where id = 2383;
select tweet from paris_all where tweet_id = 665303219317383169; 
select tweet#>>'{geo}',tweet#>>'{place,name}' from paris_all 
where tweet_id = 665304897164677120;
-----end of wierd case
--RETWEETS_API BACKUP
select * into retweets_api_backup_map_ready from retweets_api;
------ELIMINATING RETWEETS OF TWEETS ORIGINATING FROM FRANCE
select * from paris_all where tweet_id = (select tweet_id from retweets_api where id = 12053);
select count(*) --into retweets_api_france_origin_not_paris 
from retweets_api 
where exists (select 1 from paris_all where tweet#>>'{place,name}' = 'France' and tweet_id=retweets_api.tweet_id);
insert into retweets_api_france_origin_not_paris (select * from retweets_api where exists(select 1 from paris_all where tweet#>>'{place,name}'='Frankreich' and tweet_id = retweets_api.tweet_id) );
--DELETE from retweets_api where exists (select 1 from paris_all where tweet#>>'{place,name}' = 'Frankreich' and tweet_id=retweets_api.tweet_id);
select count(*) from paris_all where
select * into retweets_api_backup from retweets_api; 
drop table a1_paris_bbox; select st_envelope(st_union(st_transform(st_setsrid(geom,3857),4326)))::geometry(polygon,4326) as bbox into a1_paris_bbox from paris_fishnet;
select distinct retweet_id from retweets_api, a1_paris_bbox where st_contains(bbox,original_tweet_xy) ='f';

--delete from retweets_api where retweet_id in (select distinct retweet_id from retweets_api, a1_paris_bbox where st_contains(bbox,original_tweet_xy) ='f');

SELECT ST_CONTAINS(ST_UNION(PF.GEOM),ORIGINAL_TWEET_XY), count(ra.*)
FROM PARIS_FISHNET PF, RETWEETS_API RA
WHERE ST_CONTAINS(ST_UNION(PF.GEOM),ORIGINAL_TWEET_XY)='t';

select user_id,tweet#>>'{user,screen_name}', insert_time::date, count(distinct tweet_id) 
from paris_all 
group by user_id,tweet#>>'{user,screen_name}',insert_time::date
having count(distinct tweet_id) > 20
--order by insert_time::date desc;
select * from paris_all limit 10;

----MACHINE LEARNING FEATURE SPACE FOR SPAM DETECTION:
select user_id, tweet#>>'{user,created_at}' as created_at,tweet#>>'{user,description}' as description,tweet#>>'{user,geo_enabled}' as geo_enabled
,tweet#>>'{user,listed_count}' as listed_count,tweet#>>'{user,friends_count}' as friends_count,tweet#>>'{user,statuses_count}' as statuses_count,tweet#>>'{user,followers_count}' as followers_count,
tweet#>>'{user,favourites_count}' as favourites_count,tweet#>>'{user,location}' as location,tweet#>>'{user,lang}' as lang
from paris_all
limit 100;

select tweet_type,count(distinct tweet_id) from paris_all where tweet_type is not null group by tweet_type;
select tweet_type,count(distinct tweet_id) from retweets_api where tweet_type is not null group by tweet_type;
select tweet_type,count(distinct tweet_id) from retweets_api where tweet_type is not null and next_tweet_xy is not null group by tweet_type;

select count(id) from paris_all;

create index ix_paris_all_retweets on paris_all using btree(retweets);
create index ix_paris_all_tweet_type on paris_all using btree(tweet_type);
create index ix_paris_all_tweet_id on paris_all using btree(tweet_id);
create index ix_hashtags_paris_all_tweet_id on hashtags_paris_all using btree(tweet_id);


SELECT TWEET_ID, RETWEETS FROM paris_all WHERE RETWEETS > 0 AND RETWEETS < 25 AND NOT EXISTS (SELECT 1 FROM RETWEETS_API WHERE TWEET_ID=PARIS_ALL.TWEET_ID) ORDER BY RETWEETS DESC;

copy(select * from retweets_api) to '/tmp/retweets_api.csv' with CSV header delimiter ';';
select * into retweets_api_backup from retweets_api;

select user_id, follower_id,count(id) from followers group by user_id,follower_id having count(id) >20; --1368782430 21
select user_id, count(id) from followers where user_id = 292409371 group by user_id;
create index ix_followers_follower_id on followers using btree(follower_id);
reindex index ix_followers_user_id;

-----CLEANING UP DUPLICATES FROM FOLLOWERS TABLE
select count(id) from followers; --17.613.636  --9.379.402 after cleaning
set enable_seqscan=false;
with cte as (select id,user_id,follower_id,row_number() over(partition by user_id,follower_id order by id asc) as rn from followers)
delete from followers where id in (select id from cte where rn >1)
--Query returned successfully: 5639971 rows affected, 44.8 secs execution time. user_id = 292409371
--Query returned successfully: 2594263 rows affected, 50.2 secs execution time.
--select * from cte where follower_id = 4864 order by rn asc;

-----CLEANING UP DUPLICATES FROM FOLLOWERS_OF_FOLLOWERS TABLE
select count(id) from followers_of_followers; --465.953  --9.379.402 after cleaning
select * from followers_of_followers limit 10;
set enable_seqscan=false;
with cte as (select id,user_id,follower_id,row_number() over(partition by user_id,follower_id order by id asc) as rn from followers_of_followers)
select count(id) from cte where rn>1; --only 3. no need for cleaning....

------CHECK IF CORRECT NUMBER OF FOLLOWERS WAS DOWNLOADED IN FOF TABLE:
select count(distinct user_id) from paris_all where (hash_flag=True or tweet_type is not null or freq_words is not null);
SELECT COUNT(DISTINCT USER_ID) FROM FOLLOWERS;--3.126
SELECT COUNT(DISTINCT USER_ID) FROM retweets_api;--18.434
select count(distinct f.follower_id) from followers f, retweets_api ra where f.follower_id=ra.user_id; --12.731
SELECT COUNT(DISTINCT USER_ID) FROM FOLLOWERS_OF_FOLLOWERS;--221



set enable_seqscan=false;
select f.user_id,ra.user_id,retweet#>>'{user,screen_name}',max((retweet#>>'{user,followers_count}')::bigint),count(distinct f.follower_id) 
from followers_of_followers f, retweets_api ra 
where f.user_id=ra.user_id 
group by f.user_id,ra.user_id,retweet#>>'{user,screen_name}'--,(retweet#>>'{user,followers_count}')::bigint
order by count(distinct f.follower_id)  desc; --12.731

select retweet#>>'{user,followers_count}' from retweets_api limit 10;
create index _ix_retweets_api_user_id on retweets_api using btree(user_id);

set enable_seqscan=false;
select count(distinct tweet_id)
from retweets_api
where  not exists (select 1 from followers where follower_id=retweets_api.user_id) and hash_flag = true; 


SELECT TWEET_ID, RETWEETS FROM paris_all WHERE RETWEETS > 0 AND RETWEETS < 2 AND
             NOT EXISTS (SELECT 1 FROM RETWEETS_API WHERE TWEET_ID=PARIS_ALL.TWEET_ID) ORDER BY RETWEETS DESC;

select tweet_type,count(id) from paris_all group by tweet_type;

select id,insert_time,geom,tweet_id,useful,retweets,favourites,place_xy,support,tweet_type,user_id,journalist,hash_flag,freq_words 
into paris_all_no_tweets
from paris_all;


CREATE INDEX ix_paris_all_no_tweets_tweet_id
  ON public.paris_all_no_tweets
  USING btree
  (tweet_id);

-- Index: public.ix_paris_all_tweet_type

-- DROP INDEX public.ix_paris_all_tweet_type;

CREATE INDEX ix_paris_all_no_tweets_tweet_type
  ON public.paris_all_no_tweets
  USING btree
  (tweet_type COLLATE pg_catalog."default");

-- Index: public.ix_paris_all_useful

-- DROP INDEX public.ix_paris_all_useful;

CREATE INDEX ix_paris_all_no_tweets_useful
  ON public.paris_all_no_tweets
  USING btree
  (useful);

-- Index: public.ix_paris_all_user_id

-- DROP INDEX public.ix_paris_all_user_id;

CREATE INDEX ix_paris_all_no_tweets_user_id
  ON public.paris_all_no_tweets
  USING btree
  (user_id);

select tweet_type,hash_flag,count(id),round(avg(retweets),2) as avg_retweets,round(avg(favourites),2) as avg_favourites
from paris_all_no_tweets 
group by tweet_type,hash_flag;

select tweet_type,count(id),round(avg(retweets),2) as avg_retweets,round(avg(favourites),2) as avg_favourites
from paris_all_no_tweets
group by tweet_type;

select depth,tweet_type,count(id),round(avg(retweets),2) as avg_retweets,round(avg(favourites),2) as avg_favourites
from retweets_api
group by depth, tweet_type;

alter table retweets_api add retweets int;
alter table retweets_api add favourites int;

update retweets_api r set retweets = p.retweets from paris_all p where p.tweet_id = r.tweet_id;
update retweets_api r set favourites = p.favourites from paris_all p where p.tweet_id = r.tweet_id;

select * from retweets_api  limit 10;

alter table retweets_api add depth int;
set enable_seqscan=false;
update retweets_api r set depth = p.depth from geo_tweets_by_retweeters_complete p where p.user_id = r.user_id;

select id, hash_flag,tweet_type,freq_words,retweets,favourites
from retweets_api
where (hash_flag is not null or tweet_type is not null or freq_words is not null);


select * into retweets_api_map from retweets_api;
alter table retweets_api_map add hashtag_retweets_xy geometry(point,4326);
alter table retweets_api_map add keywords_retweets_xy geometry(point,4326);
alter table retweets_api_map add hashtag_retweets_line geometry(linestring,4326);
alter table retweets_api_map add keyword_retweets_line geometry(linestring,4326);

update retweets_api_map set hashtag_retweets_xy = first_retweet_xy where hash_flag = TRUE;
update retweets_api_map set keywords_retweets_xy = first_retweet_xy where freq_words = TRUE;
update retweets_api_map set hashtag_retweets_line = first_retweet_line where hash_flag = TRUE;
update retweets_api_map set keyword_retweets_line = first_retweet_line where freq_words = TRUE;

select st_x(first_retweet_xy),st_y(first_retweet_xy),* from retweets_api where id = 10087;

select * from retweets_api where first_retweet_xy is not null and freq_words  = TRUE order by retweets desc limit 10;

select id,retweets,favourites from retweets_api where retweets = 0;
select id,favourites from paris_all_no_tweets where freq_words = TRUE;


select distinct tweet_id from retweets_api where tweet_type is not null and not exists (select tweet_id from hashtags_paris_all where tweet_id = retweets_api.tweet_id) and freq_words is null limit 10;

select count(distinct retweet_id) from retweets_api where retweet#>>'{text}' like '%RT @%';

select * from paris_all_no_tweets where retweets is not null order by retweets desc limit 10;
select tweet#>>'{user,screen_name}' from paris_all where tweet_id = 665452815465230337;

select distinct retweet#>>'{user,location}' from retweets_api_map limit 50;
alter table retweets_api_map add retweeter_x numeric;
alter table retweets_api_map add retweeter_y numeric;
update retweets_api_map set retweeter_x=null, retweeter_y=NULl;

select id, retweet#>>'{user,location}', retweeter_x,retweeter_y 
from retweets_api_map 
where retweeter_x is null 
and retweet#>>'{user,location}'='' 
order by id asc;


update retweets_api_map set retweeter_y = 0 where retweet#>>'{user,location}'='';
alter table retweets_api_map add retweeter_xy geometry(point,4326);
update retweets_api_map set retweeter_xy = st_setsrid(st_makepoint(retweeter_x,retweeter_y),4326) where retweet#>>'{user,location}'!='';
alter table retweets_api_map add retweeter_loc_line geometry(linestring,4326);
update retweets_api_map set retweeter_loc_line = st_setsrid(st_makeline(original_tweet_xy,retweeter_xy),4326) where retweet#>>'{user,location}'!='';
alter table retweets_api_map add retweeter_loc varchar;
update retweets_api_map set retweeter_loc = retweet#>>'{user,location}';

alter table retweets_api_map add retweeter_x_gazetter varchar;
alter table retweets_api_map add retweeter_y_gazetter varchar;
alter table retweets_api_map add geom_gazetteer geometry(point,4326);
update retweets_api_map set geom_gazetteer = st_setsrid(st_makepoint(retweeter_x_gazetter::numeric,retweeter_y_gazetter::numeric),4326);

select count(retweet_id) from retweets_api_map where retweeter_x is not null;
select * from retweets_api_map order by 1 asc limit 10;

select retweet_id, tweet_id,user_id,created_at,tweet_type,hash_flag,freq_words,retweeter_screen_name,source,retweets,favourites,depth,retweeter_loc from retweets_api limit 10;

select * from retweets_api_map limit 10;

alter table hashtags_paris_all add geom geometry(point, 4326);

update hashtags_paris_all set geom = st_transform(p.geom,4326) from paris_all p where p.tweet_id=hashtags_paris_all.tweet_id;
--Query returned successfully: 337.935 rows affected, 02:03 minutes execution time.

alter table hashtags_paris_all add created_at timestamp;
update hashtags_paris_all set created_at = (p.tweet#>>'{created_at}')::timestamp from paris_all p where p.tweet_id=hashtags_paris_all.tweet_id;
--Query returned successfully: 337935 rows affected, 28:45 minutes execution time.

alter table hashtags_paris_all add country_code varchar(2);
select h.*,p.geom from hashtags_paris_all h, paris_all p where h.tweet_id=p.tweet_id limit 10;
select * from retweets_api_map where retweeter_loc = 'in the hanging tree';

select * from hashtags_paris_all limit 10;

alter table hashtags_paris_all add created_at timestamp;
update hashtags_paris_all set created_at = (p.tweet#>>'{created_at}')::timestamp from paris_all p where p.tweet_id=hashtags_paris_all.tweet_id;


SELECT * into gadm 
FROM dblink('dbname=tweets_parsing port=5432 host=myUFhadoopmaster user=scvetojevic password=myPass' ,'(select gid, iso,name_0,name_1,type_1,name_2,type_2,name_3,st_transform(geom_simplified,4326) from gadm)')
            AS s(gid int, iso varchar(3),name_0 varchar(75),name_1 varchar(75),type_1 varchar(50),name_2 varchar(75),type_2 varchar(50),name_3 varchar(75),geom_simplified geometry(multipolygon, 4326));

SELECT * into countries 
FROM dblink('dbname=tweets_parsing port=5432 host=myUFhadoopmaster user=scvetojevic password=myPass' ,'(select admin,st_transform(st_union,4326) from countries)')
            AS s(country_name varchar(75),geom_simplified geometry);

--DISTANCE BETWEEN LOCATION DISCOVERED BY USE OF GEOLOCATED TWEETS AND USER LOCATION FROM THEIR PROFILE (USE MACHINE LEARNING TO EXTRAPOLATE THIS AND CLASSIFY USERS WHOSE LOCATION FROM THEIR PROFILE CAN BE USED)
select avg(round((st_distance(st_transform(first_retweet_xy,3857),st_transform(retweeter_xy,3857))/1000)::numeric,2)) from retweets_api_map where first_retweet_xy is not null and retweeter_xy is not null order by 1 desc;
select * from retweets_api_map limit 10;
select tweet_id, hashtag, created_at
                   from hashtags_paris_all limit 10;

--DISTANCE BETWEEN LOCATION DISCOVERED BY USE OF GEOLOCATED TWEETS AND USER LOCATION FROM THEIR PROFILE (USE MACHINE LEARNING TO EXTRAPOLATE THIS AND CLASSIFY USERS WHOSE LOCATION FROM THEIR PROFILE CAN BE USED)
select user_id, source
			,max(retweets) as max_r, max(favourites) as max_f
                       ,round(avg(retweets),2) as avg_r
                       ,round(avg(favourites),2) as avg_f,
round(max(round((st_distance(st_transform(first_retweet_xy,3857),st_transform(retweeter_xy,3857))/1000)::numeric,2)),2) as max_d,
round(avg(round((st_distance(st_transform(first_retweet_xy,3857),st_transform(retweeter_xy,3857))/1000)::numeric,2)),2) as avg_d
from retweets_api_map 
where first_retweet_xy is not null 
and retweeter_xy is not null
and user_id not in (select user_id from retweets_api group by user_id having count(distinct source) >1)
group by user_id,source
order by 6 asc;

alter table hashtags_paris_all add country varchar(75);
create index ix_hashtags_paris_all_geom on hashtags_paris_all using gist(geom);
create index ix_countries_geom_simplified on countries using gist(geom_simplified);
set enable_seqscan = false;
update hashtags_paris_all set country = p.country_name from countries p where geom_simplified&&geom and st_contains(geom_simplified,geom)='t';
select country_name, st_astext(geom_simplified) from countries limit 10;

---HASHTAGS SUMMARY TABLE FOR HIERARSHICAL CLUSTERING
select h.hashtag, count(distinct h.tweet_id), max(retweets) as max_r, max(favourites) as max_f, round(avg(retweets),2) as avg_r, round(avg(favourites),2) as avg_f
from hashtags_paris_all h,paris_all p 
where h.tweet_id=p.tweet_id 
group by hashtag 
order by 2 desc;

--------HASHTAGS SPREAD ACROSS THE WORLD
SELECT TWEET_ID::BIGINT,J#>>'{text}' AS HASHTAG, created_at,  geom
INTO HASHTAGS_ALL_WORLD 
FROM (SELECT TWEET#>>'{id}' AS TWEET_ID,tweet#>>'{created_at}' as created_at,
		     ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326) as geom,
		     jsonb_array_elements(TWEET#>'{entities,hashtags}') as J
		    FROM ne_i 
		   WHERE jsonb_array_length(TWEET#>'{entities,hashtags}') > 0) AS A;

insert into HASHTAGS_ALL_WORLD
(SELECT TWEET_ID::BIGINT,J#>>'{text}' AS HASHTAG, created_at,  geom
FROM (SELECT TWEET#>>'{id}' AS TWEET_ID,tweet#>>'{created_at}' as created_at,
		     ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326) as geom,
		     jsonb_array_elements(TWEET#>'{entities,hashtags}') as J
		    FROM ne_ii 
		   WHERE jsonb_array_length(TWEET#>'{entities,hashtags}') > 0) AS A);


select * from HASHTAGS_ALL_WORLD limit 10;
create index ix_HASHTAGS_ALL_WORLD_tweet_id on HASHTAGS_ALL_WORLD using btree(tweet_id);
create index ix_HASHTAGS_ALL_WORLD_created_at on HASHTAGS_ALL_WORLD using btree(created_at);
create index ix_HASHTAGS_ALL_WORLD_geom on HASHTAGS_ALL_WORLD using gist(geom);
create index ix_gadm_geom_simplified on gadm using gist(geom_simplified);

select count(tweet_id) from HASHTAGS_ALL_WORLD; --37.424.804
select count(distinct tweet_id) from HASHTAGS_ALL_WORLD; --17.353.438

alter table HASHTAGS_ALL_WORLD add country varchar(50);

update HASHTAGS_ALL_WORLD h set country = name from esri_countries c where c.geom&&h.geom and st_contains(c.geom,h.geom)='t';

select * from countries limit 10;
select iso, name_0 as country, (st_union(geom_simplified))::geometry(multipolygon,4326) as geom
from gadm 
group by iso, name_0
order by 1 asc;

ALTER TABLE countries ALTER COLUMN geom_simplified
    SET DATA TYPE geometry(MultiPolygon,4326) USING ST_Multi(geom_simplified);

    ALTER TABLE HASHTAGS_ALL_WORLD ALTER COLUMN geom
        SET DATA TYPE geometry(Point,4326);

select * from hashtags_all_world
select st_astext(geom) from gadm28 limit 10;
create index ix_gadm28_geom on gadm28 using gist(geom);

select h.*,g.name_0 from hashtags_all_world h, gadm28 g where 
h.tweet_id = 665659764819558400 and g.geom&&h.geom and st_contains(g.geom,h.geom)='t';

alter table gadm28 add geom_simplified geometry(MultiPolygon,4326);
update gadm28 set geom_simplified = st_snaptogrid(geom,0.001);
create index ix_gadm28_geom_simplified on gadm28 using gist(geom_simplified);

select st_astext(geom),st_astext(geom_simplified) from gadm28 limit 30;


create index ix_esri_countries_geom on esri_countries using gist(geom);
select name from esri_countries limit 10;

set enable_seqscan=false;
update HASHTAGS_ALL_WORLD h set country = name_0 from gadm28 c where c.geom_simplified&&h.geom and st_contains(c.geom_simplified,h.geom)='t';

----DISTANCE BETWEEN USER PROFILE LOCATION AND GEOCODED TWEETS LOCATION. THIS IS CORRELATION TABLE:
select user_id, source,max(retweets) as max_r, max(favourites) as max_f
,round(avg(retweets),2) as avg_r,round(avg(favourites),2) as avg_f,
round(max(round((st_distance(st_transform(first_retweet_xy,3857),st_transform(retweeter_xy,3857))/1000)::numeric,2)),2) as max_d,
round(avg(round((st_distance(st_transform(first_retweet_xy,3857),st_transform(retweeter_xy,3857))/1000)::numeric,2)),2) as avg_d
,max((retweet#>>'{user,friends_count}')::int) as friends_count, max((retweet#>>'{user,statuses_count}')::int) as statuses_count, max((retweet#>>'{user,followers_count}')::int) as followers_count
,max((EXTRACT(epoch FROM (created_at-(retweet#>>'{user,created_at}')::timestamp))/86400)::int) as days_old
from retweets_api_map 
where first_retweet_xy is not null and retweeter_xy is not null
and user_id not in (select user_id from retweets_api group by user_id having count(distinct source) >1)
group by user_id,source order by 1 asc;
----DISTANCE BETWEEN USER PROFILE LOCATION AND GEOCODED TWEETS LOCATION.

select created_at-(retweet#>>'{user,created_at}')::timestamp 
,(EXTRACT(epoch FROM (created_at-(retweet#>>'{user,created_at}')::timestamp))/86400)::int
from retweets_api_map limit 10;

select hashtag,count(tweet_id) 
from hashtags_all_world 
where hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan')
group by hashtag
order by 2 desc
limit 30;


select * into hashtags_paris_world from hashtags_all_world where hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan');

select country,count(distinct tweet_id) 
from hashtags_paris_world 
where country is not null 
group by country
order by 2 desc;
select count(distinct tweet_id) from hashtags_paris_world where country is null;

create index ix_hashtags_paris_world_geom on hashtags_paris_world using gist(geom);
alter table hashtags_paris_world drop column country;
alter table hashtags_paris_world add time_windows_6_h int;
alter table hashtags_paris_world add country varchar(50);

set enable_seqscan = FALSE;
update hashtags_paris_world h set country = name from esri_countries c where c.geom&&h.geom and st_contains(c.geom,h.geom)='t';

select * from esri_countries limit 10;
select gid,name,geom,(select count(distinct tweet_id) from hashtags_paris_world where )

select name,count(h.tweet_id)
from esri_countries e, hashtags_paris_world h
where name='France' and e.geom&&h.geom and st_contains(e.geom,h.geom)='t'
group by name;

select max(created_at::timestamp) from hashtags_paris_world;


select *, created_at::timestamp-'2015-11-13 20:32:11'::timestamp, 
((extract(hour from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int)/6 
from hashtags_paris_world 
order by created_at::timestamp asc limit 50;



drop table tst;
select country,(select st_union(geom) from esri_countries where name=hashtags_paris_world.country),time_windows_6_h,count(tweet_id) as count
into tst
from hashtags_paris_world 
where country in('Germany','France','Italy')
group by country,time_windows_6_h 
order by 3 asc;


select name, st_setsrid(st_union(geom),4326) into esri_countries_single_geom from esri_countries group by name order by 1 asc;
alter table esri_countries_single_geom alter column st_setsrid type geometry(MultiPolygon, 4326);

alter table esri_countries_single_geom add column three_h_1 int;
update esri_countries_single_geom e set three_h_1 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 0 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_2 int;
update esri_countries_single_geom e set three_h_2 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 1 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_3 int;
update esri_countries_single_geom e set three_h_3 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 2 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_4 int;
update esri_countries_single_geom e set three_h_4 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 3 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_5 int;
update esri_countries_single_geom e set three_h_5 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 4 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_6 int;
update esri_countries_single_geom e set three_h_6 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 5 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_7 int;
update esri_countries_single_geom e set three_h_7 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 6 group by country) as t where t.country = e.name;

alter table esri_countries_single_geom add column three_h_8 int;
update esri_countries_single_geom e set three_h_8 = cnt from (select count(distinct tweet_id) as cnt,country from hashtags_paris_world where time_windows_6_h = 7 group by country) as t where t.country = e.name;

drop table if exists knox_Bataclan;
with cte as (
select tweet_id, round(st_x(st_snaptogrid(geom,0.01))::numeric,2) as X, round(st_y(st_snaptogrid(geom,0.01))::numeric,2) as Y, created_at::date as date,st_snaptogrid(geom,0.01) AS GEOM 
from hashtags_paris_world where hashtag = 'Bataclan')
select (row_number() over(order by x,y asc))::real as id, X::real,Y::real,DATE,GEOM,(COUNT(TWEET_ID))::real  into knox_Bataclan
FROM CTE GROUP BY X,Y,DATE,GEOM;
--('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan')
select created_at::date,hashtag,count(distinct tweet_id)
from hashtags_paris_world
group by created_at::date,hashtag
order by 1,2 asc;

create table retweeters(retweeters_id serial, tweet_id bigint, retweeter_id bigint);
select * from retweeters;

update hashtags_paris_world set time_windows_6_h = ((extract(day from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int)*24+((extract(hour from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int);
select *,date_trunc('hour',created_at::timestamp) from hashtags_paris_world limit 10;
alter table hashtags_paris_world add date_trunc_h timestamp;
update hashtags_paris_world set date_trunc_h = date_trunc('hour',created_at::timestamp);
select date_trunc_h,hashtag,count(distinct tweet_id) from hashtags_paris_world group by date_trunc_h,hashtag order by 1,2 asc;

create index ix_gadm_name_0 on gadm28 using btree(name_0);
select distinct name_1 from gadm28 where name_0 = 'France';

with cte as(
select date_trunc_h as dh,hashtag,count(distinct tweet_id) 
                     from hashtags_paris_world 
                     group by date_trunc_h,hashtag 
                    order by 1,2 asc)
                    select dh,hashtag,count,coalesce(lag(count) over(partition by hashtag order by dh asc),0) as t_1,
                    (coalesce(lag(count) over(partition by hashtag order by dh asc),0)^2) as t_1_sq
                    from cte order by dh asc;

select * into hashtags_fusillade from hashtags_paris_world where hashtag = 'fusillade';
create index ix_hashtags_paris_world_hashtag on hashtags_paris_world using btree(hashtag);


select * from hashtags_fusillade limit 10;

update hashtags_paris_world set time_windows_6_h = 
	((extract(day from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int)*24+
	((extract(hour from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int);

update hashtags_fusillade set date_trunc_h = date_trunc('day',created_at::timestamp);


select time_windows_6_h, count(tweet_id) from hashtags_fusillade group by time_windows_6_h order by 1 asc;

select hashtag,time_windows_6_h, count(tweet_id) 
from hashtags_paris_world 
where hashtag = 'PrayForParis' and country = 'United States'
group by hashtag, time_windows_6_h
order by 2 asc ;

select * into hashtags_fusillade from hashtags_paris_world where hashtag = 'fusillade';
create index ix_hashtags_paris_world_hashtag on hashtags_paris_world using btree(hashtag);


select hashtag,country,count(tweet_id) 
from hashtags_paris_world
group by hashtag,country
order by 3 desc;

update hashtags_paris_world set time_windows_6_h = 
	((extract(day from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int)*24+
	((extract(hour from (created_at::timestamp-'2015-11-13 20:32:11'::timestamp)))::int);

update hashtags_fusillade set date_trunc_h = date_trunc('day',created_at::timestamp);


select time_windows_6_h, count(tweet_id) from hashtags_fusillade group by time_windows_6_h order by 1 asc;


select hashtag,time_windows_6_h as hour, coalesce(lag(count(tweet_id)) over(order by time_windows_6_h asc),0) as count
from hashtags_paris_world 
where hashtag = 'PrayForParis' and country = 'United States'
group by hashtag, time_windows_6_h
order by 2 asc;

with cte as (
select hashtag,time_windows_6_h as hour, coalesce(lag(count(tweet_id)) over(order by time_windows_6_h asc),0) as count
from hashtags_paris_world 
where hashtag = 'PrayForParis' and country = 'United States'
group by hashtag, time_windows_6_h
order by 2 asc)
select hour, count,sum(count)over(order by hour), ((sum(count)over(order by hour))^2)::integer as cum_t_1
from cte;


select * from hashtags_paris_world limit 10;

alter table hashtags_paris_world add date_trunc_m timestamp;
update hashtags_paris_world set date_trunc_m = date_trunc('minute',created_at::timestamp);

select date_trunc_m,count(tweet_id)
from hashtags_paris_world
where hashtag = 'PrayForParis' and country = 'United States'
group by date_trunc_m
order by 1 asc;



with cte as (
                      select time_windows_6_h as hour, 
                      count(tweet_id) as count, 
                      lag(count(tweet_id)) over(order by time_windows_6_h asc) as lag_count
                      from hashtags_paris_world 
                      where hashtag = 'PrayForParis' and country = 'Germany'
                      group by time_windows_6_h
                      order by time_windows_6_h asc)
                      select hour,
                      count, 
                      coalesce(sum(lag_count) over(order by hour asc),0) as cum_lag_count, 
                      ((coalesce(sum(lag_count) over(order by hour asc),0))^2)::integer as cum_lag_squared
                      from cte;

select time_windows_6_h as hour, 
                       count(tweet_id) as count
                       from hashtags_paris_world 
                       where hashtag = 'PrayForParis' 
                       and country = 'France'
                       group by time_windows_6_h
                       order by time_windows_6_h asc;

select * into hashtags_paris_world from hashtags_all_world where hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan');
select * into hashtags_twitter_world from hashtags_all_world where hashtag in ('Hiring','Job','job','MTVStars','Jobs','CareerArc','hiring','Veterans','trndnl','Retail','Hospitality','MadeInTheAM','AMAs','FR1DAY13BR','Sales','Nursing');
alter table hashtags_twitter_world add time_windows_6_h int;
update hashtags_twitter_world set time_windows_6_h = 
	((extract(day from (created_at::timestamp-'2015-11-13 00:00:00'::timestamp)))::int)*24+
	((extract(hour from (created_at::timestamp-'2015-11-13 00:00:00'::timestamp)))::int);


select * from hashtags_paris_world limit 10;

select hashtag, country, count(tweet_id) 
from hashtags_all_world
where country is not null
group by hashtag, country
order by 3 desc
limit 1000;

select * from hashtags_twitter_world where hashtag = 'Thanksgiving' 
                       and country = 'United States'
select time_windows_6_h as hour, 
                       count(tweet_id) as count
                       from hashtags_twitter_world 
                       where hashtag = 'Thanksgiving' 
                       and country = 'United States'
                       group by time_windows_6_h
                       order by time_windows_6_h asc;


                       select time_windows_6_h as hour, 
                       count(tweet_id) as count
                       from hashtags_twitter_world 
                       where hashtag = 'MadeInTheAM' 
                       --and country = 'United States'
                       group by time_windows_6_h
                       order by time_windows_6_h asc;

--#############URLS internal influence:
insert into HASHTAGS_ALL_WORLD
(SELECT TWEET_ID::BIGINT,J#>>'{text}' AS HASHTAG, created_at,  geom
FROM (SELECT TWEET#>>'{id}' AS TWEET_ID,tweet#>>'{created_at}' as created_at,
		     ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326) as geom,
		     jsonb_array_elements(TWEET#>'{entities,hashtags}') as J
		    FROM ne_ii 
		   WHERE jsonb_array_length(TWEET#>'{entities,hashtags}') > 0) AS A);

SELECT TWEET_ID::BIGINT,J#>>'{expanded_url}' AS URL, created_at,  geom
into URLS_ALL_WORLD
FROM (SELECT TWEET#>>'{id}' AS TWEET_ID,tweet#>>'{created_at}' as created_at,
		     ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326) as geom,
		     jsonb_array_elements(TWEET#>'{entities,urls}') as J
		    FROM ne_i 
		   WHERE jsonb_array_length(TWEET#>'{entities,urls}') > 0) AS A;

INSERT INTO URLS_ALL_WORLD
(SELECT TWEET_ID::BIGINT,J#>>'{expanded_url}' AS URL, created_at,  geom
 FROM (SELECT TWEET#>>'{id}' AS TWEET_ID,tweet#>>'{created_at}' as created_at,
		     ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326) as geom,
		     jsonb_array_elements(TWEET#>'{entities,urls}') as J
		    FROM ne_ii 
		   WHERE jsonb_array_length(TWEET#>'{entities,urls}') > 0) AS A);

INSERT INTO URLS_ALL_WORLD
(SELECT TWEET_ID::BIGINT,J#>>'{expanded_url}' AS URL, created_at,  geom
 FROM (SELECT TWEET#>>'{id}' AS TWEET_ID,tweet#>>'{created_at}' as created_at,
		     ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(tweet#>>'{coordinates}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326) as geom,
		     jsonb_array_elements(TWEET#>'{entities,urls}') as J
		    FROM nw_i
		   WHERE jsonb_array_length(TWEET#>'{entities,urls}') > 0) AS A);

select * from URLS_ALL_WORLD limit 10;
select count(tweet_id) from urls_all_world;

create index ix_URLS_ALL_WORLD_created_at on URLS_ALL_WORLD using btree(created_at);
create index ix_URLS_ALL_WORLD_url on URLS_ALL_WORLD using btree(url);

alter table urls_all_world add urls_tsvector tsvector;
update urls_all_world set urls_tsvector = to_tsvector('english',url);
select url, to_tsvector('english',url) from urls_all_world limit 10;

--drop table hashtags_prayforparis_world;
select * into hashtags_prayforparis_world from hashtags_paris_world where hashtag = 'PrayForParis' and time_windows_6_h < 100; 

select * from hashtags_prayforparis_world where time_windows_6_h = 0;

select count(tweet_id) from urls_all_world where urls_tsvector@@to_tsquery('cnn');
select url from urls_all_world where urls_tsvector @@ to_tsquery('%cnn.com%') limit 10;
select * from urls_all_world limit 10;


alter table urls_all_world add time_windows_6_h int;
alter table urls_all_world add country varchar(50);

create index ix_urls_all_world_geom on urls_all_world using gist(geom);
set enable_seqscan = FALSE;
update urls_all_world h set country = name from esri_countries c where c.geom&&h.geom and st_contains(c.geom,h.geom)='t';

update urls_all_world set time_windows_6_h = 
	((extract(day from (created_at::timestamp-'2015-11-13 00:00:00'::timestamp)))::int)*24+
	((extract(hour from (created_at::timestamp-'2015-11-13 00:00:00'::timestamp)))::int);

select * from hashtags_prayforparis_world limit 10;

select st_centroid( from hashtags_prayforparis_world where time_windows_6_h = 0;

alter table hashtags_paris_world add d numeric;

create index ix_hashtags_paris_world_country on hashtags_paris_world using btree(country);
select st_astext(geom) from hashtags_paris_world limit 10;
update hashtags_paris_world set d = st_distance(st_transform(st_setsrid(st_makepoint(2.3508, 48.8567),4326),3857), st_transform(geom,3857)) where country = 'France';

select d::int, count(tweet_id) 
from hashtags_paris_world where  hashtag = 'PrayForParis' 
group by d::int
order by 2 desc;

select st_distance(st_transform(st_setsrid(st_makepoint(2.3508, 48.8567),4326),3857), st_transform(geom,3857)) from hashtags_paris_world where tweet_id = 665295029485875200;

--drop table hashtag_country_count;
SELEct distinct name,geom into hashtag_country_count
from esri_countries order by 1 asc;
create index ix_hashtag_country_count_geom on hashtag_country_count using gist(geom);

alter table hashtag_country_count add PrayForParis_3h int;
update hashtag_country_count h set PrayForParis_3h = (select count(tweet_id) from hashtags_paris_world where hashtag = 'PrayForParis' and country = h.name  and time_windows_6_h < 4);

select name,PrayForParis_3h from hashtag_country_count limit 100;
select st_astext(geom_simplified) from gadm limit 10;

select count(*) from hashtags_all_world;
SELECT UpdateGeometrySRID('gadm','geom_simplified',4326);
select * from hashtags_all_world limit 10;
create index ix_hashtags_all_world_hashtag on hashtags_all_world using btree(hashtag);
alter table hashtags_all_world add gadm_gid int;
st_transform(st_setsrid(geom_simplified,2249),4326);
update hashtags_all_world h set gadm_gid = gid from gadm28 g where hashtag = 'PrayForParis' and g.geom_simplified&&h.geom and st_contains(g.geom_simplified,h.geom)='t';

select st_astext(geom) from hashtags_all_world where hashtag = 'PrayForParis' limit 10;

select st_astext(geom),st_astext(geom_simplified) from gadm28 where name_0 = 'Monaco'

select * into hashtags_01st_h from hashtags_paris_world where time_windows_6_h =1;
select * into hashtags_02nd_h from hashtags_paris_world where time_windows_6_h =2;
select * into hashtags_03rd_h from hashtags_paris_world where time_windows_6_h =3;
select * into hashtags_04th_h from hashtags_paris_world where time_windows_6_h =4;
select * into hashtags_05th_h from hashtags_paris_world where time_windows_6_h =5;
select * into hashtags_06th_h from hashtags_paris_world where time_windows_6_h =6;


select * into hashtags_6_h from hashtags_paris_world where time_windows_6_h <7;
select * into hashtags_12_h from hashtags_paris_world where time_windows_6_h >6 and time_windows_6_h <13;
select * into hashtags_18_h from hashtags_paris_world where time_windows_6_h >12 and time_windows_6_h <19;
select * into hashtags_24_h from hashtags_paris_world where time_windows_6_h >18 and time_windows_6_h <25;

select * into hashtags_30_h from hashtags_paris_world where time_windows_6_h >24 and time_windows_6_h <31;
select * into hashtags_36_h from hashtags_paris_world where time_windows_6_h >30 and time_windows_6_h <37;
select * into hashtags_42_h from hashtags_paris_world where time_windows_6_h >36 and time_windows_6_h <43;
select * into hashtags_48_h from hashtags_paris_world where time_windows_6_h >42 and time_windows_6_h <49;
select * into hashtags_54_h from hashtags_paris_world where time_windows_6_h >48 and time_windows_6_h <55;
select * into hashtags_60_h from hashtags_paris_world where time_windows_6_h >54 and time_windows_6_h <61;

select * into hashtags_66_h from hashtags_paris_world where time_windows_6_h >60 and time_windows_6_h <67;
select * into hashtags_72_h from hashtags_paris_world where time_windows_6_h >66 and time_windows_6_h <73;
select * into hashtags_78_h from hashtags_paris_world where time_windows_6_h >72 and time_windows_6_h <79;
select * into hashtags_84_h from hashtags_paris_world where time_windows_6_h >78 and time_windows_6_h <85;

select * from cities limit 10;
select * from hashtags_paris_world limit 10;
alter table hashtags_paris_world drop column d;
alter table hashtags_paris_world add column city_gid int;

select gid from cities c, hashtags_paris_world h where h.tweet_id = 665535528817852416 and st_buffer(c.geom,0.2)&&h.geom and st_contains(st_buffer(c.geom,0.2),h.geom) = 't';
create index ix_cities_geom on cities using gist(geom);

set enable_seqscan = FALSE;
update hashtags_paris_world h set city_gid = gid from cities c where h.hashtag = 'PrayForParis' and st_buffer(c.geom,0.2)&&h.geom and st_contains(st_buffer(c.geom,0.2),h.geom) = 't';

/*************************************************COUNTING HASHTAGS IN CITIES FOR REGRESSION**************************************************************************************/
select gid,city_name,fips_cntry,status,geom
	,(select count(distinct tweet_id) 
	  from hashtags_paris_world h 
	  where hashtag = 'PrayForParis'
	  and st_buffer(c.geom,0.2)&&h.geom 
	  and st_contains(st_buffer(c.geom,0.2),h.geom) = 't') 
from cities c
where city_name = 'Paris';
select * from cities limit 10;

drop table hashtags_regression_prayforparis;
select gid,city_name,fips_cntry,status,c.geom,time_windows_6_h,case when status ilike '%national%' then 1
											when status ilike '%provincial%' then 2
											else 3 end as city_rank,pop,
											count(distinct tweet_id) 
into hashtags_regression_prayforparis 
from cities c, hashtags_paris_world h
where hashtag = 'PrayForParis'  and st_buffer(c.geom,0.5)&&h.geom 
	  and st_contains(st_buffer(c.geom,0.5),h.geom) = 't'
	  group by gid,city_name,fips_cntry,status,c.geom,time_windows_6_h,status,pop;
	  
--Query returned successfully: 12241 rows affected, 09:04 minutes execution time.
											
select * from hashtags_regression_prayforparis where fips_cntry = 'BR';
/******************************CHECK HOW MANY HASHTAGS WERE LEFT OUT***********************************************************/
SELECT SUM(COUNT) FROM hashtags_regression_prayforparis; --51965
SELECT COUNT(DISTINCT TWEET_ID) from hashtags_paris_world where hashtag = 'PrayForParis'; --97542
select * from cities;
select distinct sub.status from (select fips_cntry,status,count(distinct gid) from cities group by fips_cntry,status having count(distinct gid) > 1) as sub;

select * from cities where status = 'National capital and provincial capital enclave';
	  
/*********************************************************************************************************NEAREST NEIGHBORS TABLE******************************************************************************/
alter table cities add nn_gid int;
alter table cities add nn_city_name varchar(30);
alter table cities add city_rank int;
update cities c1 set city_rank = sub.rank from (select gid,row_number() over (partition by cntry_name order by pop desc) as rank
					       from cities c2) as sub where sub.gid = c1.gid;
update hashtags_regression_prayforparis h set city_rank = cities.city_rank from cities where cities.gid=h.gid;

update cities c1 set nn_city_name = (select c2.city_name from cities c2 where c1.fips_cntry=c2.fips_cntry and c1.gid<>c2.gid ORDER BY ST_Distance(c1.geom,c2.geom) limit 1);
SELECT g1.gid As gref_gid, g1.city_name As gref_description, 
g2.gid As gnn_gid, g2.city_name As gnn_description 
    FROM cities As g1, cities As g2   
WHERE g1.fips_cntry = 'IT' and g2.fips_cntry = 'IT' and g1.gid <> g2.gid  
ORDER BY ST_Distance(g1.geom,g2.geom) limit 10;
update cities set pop = 6000000 where city_name = 'Miami' and admin_name = 'Florida';
update cities set pop = 18600000 where city_name = 'Los Angeles' and admin_name = 'California';
update cities set pop = 9500000 where city_name = 'Chicago' and admin_name = 'Illinois';

select city_name,admin_name,status,nn_city_name,pop,row_number() over (partition by cntry_name order by pop desc), city_rank
from cities where cntry_name = 'United States' and admin_name = 'Florida';
/************************************************************************************************************NEAREST NEIGHBORS TABLE******************************************************************************/
select * from cities limit 10;
select * from hashtags_regression_prayforparis where city_name = 'Mexico City' limit 100;
select * from cities where city_name = 'Mexico City';

alter table hashtags_regression_prayforparis add cntry_name varchar(30);
update hashtags_regression_prayforparis h set cntry_name = cities.cntry_name from cities where cities.gid=h.gid;

alter table hashtags_regression_prayforparis add delta_capital int;
update hashtags_regression_prayforparis set delta_capital=(select h1.city_name,h1.count-h2.count 
										    from hashtags_regression_prayforparis h1,hashtags_regression_prayforparis h2
										    where h1.cntry_name=h2.cntry_name and h1.city_rank=h2.city_rank-1 and h1.city_rank > 1
										    and h1.time_windows_6_h = h2.time_windows_6_h-1
										    and h1.cntry_name = 'Mexico' and h2.cntry_name = 'Mexico' limit 1);

select h1.city_nameh1.count-h2.count
										    from hashtags_regression_prayforparis h1,hashtags_regression_prayforparis h2
										    where h1.cntry_name=h2.cntry_name and h1.city_rank=h2.city_rank-1 and h1.city_rank > 1
										    and h1.time_windows_6_h = h2.time_windows_6_h-1
										    and h1.cntry_name = 'Mexico' and h2.cntry_name = 'Mexico'
select * from hashtags_regression_prayforparis where city_name = 'Miami';

alter table hashtags_regression_prayforparis add cum_count_t_1 int;

/******************************************************************************CUMULATIVE COUNT FOR T-1*********************************************************************************************************************/
with cte as (
select * , sum(count) over(partition by gid order by time_windows_6_h asc) as cum_count
from hashtags_regression_prayforparis)
update hashtags_regression_prayforparis h set cum_count_t_1 =c1.cum_count_t_1 from (select gid,time_windows_6_h,coalesce((lag(cum_count) over(partition by gid order by time_windows_6_h asc)::int),0) as cum_count_t_1
from cte c) as c1 where h.gid=c1.gid and h.time_windows_6_h=c1.time_windows_6_h;
/******************************************************************************CUMULATIVE COUNT FOR T-1*********************************************************************************************************************/
alter table hashtags_regression_prayforparis add d_from_paris_km int;
update hashtags_regression_prayforparis set  d_from_paris_km = st_distance(st_transform(geom,3857),st_transform((select geom from cities where city_name='Paris'),3857))/1000  where city_rank =1; --geodetic ditsance
update hashtags_regression_prayforparis h set  d_from_paris_km = st_distance(st_transform(geom,3857),st_transform((select geom from cities where city_rank=1 and cntry_name = h.cntry_name),3857))/1000  
where city_rank!=1;
SELECT * FROM hashtags_regression_prayforparis where city_name = 'Boston';
SELECT city_name,time_windows_6_h,city_rank,pop,count,cum_count_t_1 FROM hashtags_regression_prayforparis;

select * from hashtags_regression_prayforparis limit 10;
/******************************************************************************DELTA FOR CAPITAL*****************************SHOULD BE COUNT FOR T OR CUMULATIVE T MINUS CUMULATIVE T-1****************************************************************************************/
ALTER TABLE hashtags_regression_prayforparis ADD largest_city_in_country_delta int;

with cte as (
select gid,city_name,cntry_name,city_rank,time_windows_6_h,count, (select count from hashtags_regression_prayforparis h1
		where h1.cntry_name=h2.cntry_name and h1.time_windows_6_h=h2.time_windows_6_h and h1.city_rank=1) as capital_count
from hashtags_regression_prayforparis h2)

update hashtags_regression_prayforparis h 
set largest_city_in_country_delta =c1.capital_lag from (select gid,cntry_name,city_rank,city_name,time_windows_6_h,count,capital_count -
										      coalesce(lag(capital_count) over(partition by cntry_name,city_name order by time_windows_6_h asc),0) as capital_lag
from cte order by time_windows_6_h,city_name asc --where  time_windows_6_h < 10 and city_name in ('Manchester','Liverpool','Birmingham')
) as c1 where h.gid=c1.gid and h.time_windows_6_h=c1.time_windows_6_h;
/******************************************************************************DELTA FOR CAPITAL*********************************************************************************************************************/
select count(*) from hashtags_regression_prayforparis;
/******************************************************************************TWITTER PENETRATION RATE*********************************************************************************************************************/
drop table hashtags_analysis_tweets_counts_per_city;
select gid,city_name,fips_cntry,status,c.geom,case when status ilike '%national%' then 1
											when status ilike '%provincial%' then 2
											else 3 end as city_rank,pop,
											count(id) as ne_i
into hashtags_analysis_tweets_counts_per_city
from cities c, ne_i h
where st_buffer(c.geom,0.5)&&(ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(h.tweet#>>'{geo}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(h.tweet#>>'{place,bounding_box}')))),4326))
	  and st_contains(st_buffer(c.geom,0.5),((ST_SETSRID(COALESCE(ST_GeomFromGeoJSON(h.tweet#>>'{geo}'), ST_CENTROID(ST_ENVELOPE(ST_GeomFromGeoJSON(h.tweet#>>'{place,bounding_box}')))),4326)))) = 't'
	  group by gid,city_name,fips_cntry,status,c.geom,status,pop;
select * from ne_i limit 10;	  
--Query returned successfully: 12241 rows affected, 09:04 minutes execution time.
select * from ne_i a, ne_i_places b where a.id=b.id


select * from hashtags_paris_world limit 10;

select place#>>'{country}' as country,place#>>'{country_code}' as country_code, place#>>'{place_type}'  as place_type,place#>>'{name}' as place_name,(st_setsrid(st_makevalid((ST_ENVELOPE(ST_GeomFromGeoJSON(place#>>'{bounding_box}')))),4326))::geometry as geom,hashtag,count(h.tweet_id)
into nw_i_places_paris_hashtags
from nw_i_places a, hashtags_paris_world h 
where a.tweet_id::bigint=h.tweet_id
group by place#>>'{country}',place#>>'{country_code}', place#>>'{place_type}',place#>>'{name}',(st_setsrid(st_makevalid((ST_ENVELOPE(ST_GeomFromGeoJSON(place#>>'{bounding_box}')))),4326))::geometry,hashtag;


select tweet#>>'{place,country}' as country
	 ,tweet#>>'{place,country_code}' as country_code
	 ,tweet#>>'{place,place_type}'  as place_type
	 ,tweet#>>'{place,name}' as place_name
	 ,(st_setsrid(st_makevalid((ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326))::geometry as geom
	 ,case when length(tweet#>>'{source}')>0  
		then substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) 
		else 'no source'
		end as source
	 ,hashtag
	 ,date_trunc('hour',(tweet#>>'{created_at}')::timestamp) as hour
	 ,count(h.tweet_id)
into se_places_paris_hashtags
from se a, hashtags_paris_world h 
where (a.tweet#>>'{id}')::bigint=h.tweet_id
group by tweet#>>'{place,country}'
	      ,tweet#>>'{place,country_code}'
	      ,tweet#>>'{place,place_type}'
	      ,tweet#>>'{place,name}'
	      ,(st_setsrid(st_makevalid((ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326))::geometry
	      ,case when length(tweet#>>'{source}')>0  
		then substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) 
		else 'no source'
		end
	      ,hashtag
	      ,date_trunc('hour',(tweet#>>'{created_at}')::timestamp);

select tweet#>>'{place,country}' as country
	 ,tweet#>>'{place,country_code}' as country_code
	 ,tweet#>>'{place,place_type}'  as place_type
	 ,tweet#>>'{place,name}' as place_name
	 ,(st_setsrid(st_makevalid((ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326))::geometry as geom
	 ,date_trunc('hour',(tweet#>>'{created_at}')::timestamp) as hour
	 ,case when length(tweet#>>'{source}')>0  
		then substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) 
		else 'no source'
		end as source
	 ,count(id)
into ne_i_places_counts
from ne_i a 
group by tweet#>>'{place,country}'
	      ,tweet#>>'{place,country_code}'
	      ,tweet#>>'{place,place_type}'
	      ,tweet#>>'{place,name}'
	      ,(st_setsrid(st_makevalid((ST_ENVELOPE(ST_GeomFromGeoJSON(tweet#>>'{place,bounding_box}')))),4326))::geometry
	      ,date_trunc('hour',(tweet#>>'{created_at}')::timestamp)
	      ,case when length(tweet#>>'{source}')>0  
		then substring(TWEET#>>'{source}',position('>' in TWEET#>>'{source}')+1,position('</' in TWEET#>>'{source}')-position('>' in TWEET#>>'{source}')-1) 
		else 'no source'
		end;

select country,place_type,place_name,sum(count)
from nw_iii_places_paris_hashtags
group by country,place_type,place_name
order by 1,2,3 asc;


select tweet#>>'{place,country}'
	      ,tweet#>>'{place,country_code}'
	      ,tweet#>>'{place,place_type}'
	      ,tweet#>>'{place,name}' 
	      from ne_i where geom is not null limit 1000;

select * into hashtgas_paris_places_counts from ne_i_places_paris_hashtags
union all select * from ne_ii_places_paris_hashtags
union all select * from nw_i_places_paris_hashtags
union all select * from nw_ii_places_paris_hashtags
union all select * from nw_iii_places_paris_hashtags
union all select * from se_places_paris_hashtags
union all select * from sw_places_paris_hashtags;

select * into hashtgas_places_tweets_counts from ne_i_places_counts_source
union all select * from ne_ii_places_source_counts
union all select * from nw_i_places_source_counts
union all select * from nw_ii_places_source_counts
union all select * from nw_iii_places_source_counts
union all select * from se_places_source_counts
union all select * from sw_places_source_counts;

select country,country_code,place_name,source,hour,(select sum(count) from hashtgas_paris_places_counts h 
										 where h.country_code=t.country_code and h.place_type=t.place_type and h.place_name=t.place_name and h.hour=t.hour
										 and h.source=t.source) 
from hashtgas_places_tweets_counts t
where t.country_code='FR'
order by 1,2,3,4,5 asc;


select t.country,t.country_code,t.place_name,t.place_type,t.source,t.hour, h.hashtag, sum(t.count) as tweets_count, sum(h.count) as hashtags_count
into hashtags_regression_table
from hashtgas_places_tweets_counts t, hashtgas_paris_places_counts h
where h.country_code=t.country_code and h.place_type=t.place_type and h.place_name=t.place_name and h.hour=t.hour and h.source=t.source
group by t.country,t.country_code,t.place_name,t.place_type,t.source,t.hour, h.hashtag
order by 1,2,3,4,5,6,7 asc;


create index ix_ne_i_places_counts_source_geom on ne_i_places_counts_source using gist(geom);
create index ix_ne_i_places_counts_source_country_code on ne_i_places_counts_source using btree(country_code);
create index ix_ne_i_places_counts_source_place_type on ne_i_places_counts_source using btree(place_type);
create index ix_ne_i_places_counts_source_place_name on ne_i_places_counts_source using btree(place_name);

hashtags_regression_table

create index ix_hashtags_regression_table_geom on hashtags_regression_table using gist(geom);
create index ix_hashtags_regression_table_country_code on hashtags_regression_table using btree(country_code);
create index ix_hashtags_regression_table_place_type on hashtags_regression_table using btree(place_type);
create index ix_hashtags_regression_table_place_name on hashtags_regression_table using btree(place_name);


with cte as(
select a.place_name,b.place_name,st_distance_sphere(st_centroid(a.geom),st_centroid(b.geom)) as d
from ne_i_places_counts_source a, ne_i_places_counts_source b
where a.country_code = 'FR' and b.country_code = 'FR'
and a.place_type = 'city' and b.place_type='city'
and a.place_name != b.place_name
and st_distance_sphere(st_centroid(a.geom),st_centroid(b.geom)) < 20 limit 10
)
select count(*) from cte;

select pace_name, from hashtags_regression_table limit 10;

create index ix_hashtags_regression_table_geom on hashtags_regression_table using gist(geom);
create index ix_hashtags_regression_table_country_code on hashtags_regression_table using btree(country_code);
create index ix_hashtags_regression_table_place_type on hashtags_regression_table using btree(place_type);
create index ix_hashtags_regression_table_place_name on hashtags_regression_table using btree(place_name);

with cte as(
select country_code,place_name,geom--,sum(hashtags_count) 
from hashtags_regression_table --where country_code = 'FR'
group by country_code,place_name,geom
having sum(hashtags_count) > 10
order by 1,2 asc)
select a.place_name,b.place_name,round(st_distance_sphere(st_centroid(a.geom),st_centroid(b.geom))::numeric,0)
from cte a, cte b
where a.country_code = 'FR' and a.country_code=b.country_code
and a.place_name != b.place_name
and round(st_distance_sphere(st_centroid(a.geom),st_centroid(b.geom))::numeric,0) < 30000;

/*---------------------------------------------------VERY COOL FUNCTION FOR DISTANCE BASED CLUSTERING------------------------------------------------------------*/
--select * into hashtags_regression_table_useful from hashtags_regression_table where place_type not in ('country','admin') and source in ('Twitter for iPhone','Twitter for Android','Twitter for iPad','Twitter for Windows Phone','Twitter for BlackBerry®','Twitter for BlackBerry','iOS','Tweetbot for iΟS','Twitter for  Android');
drop table hashtags_st_clusterwithin_01;
with cte as(
select row_number() over (ORDER BY country_code,place_name asc) as id, country_code,place_name,geom--,sum(hashtags_count) 
from hashtags_regression_table_useful --where country_code = 'FR'
group by country_code,place_name,geom
having sum(hashtags_count) >10
order by 1,2 asc)
--select * into hashtags_for_get_domains_n from cte;
--SELECT gm into hashtags_st_clusterwithin_01_france FROM get_domains_n('hashtags_for_get_domains_n', 'geom', 'id', 0.05) AS g(gm geometry);
--select source, sum(hashtags_count) from hashtags_regression_table group by source order by 2 desc;
--select place_type, sum(hashtags_count) from hashtags_regression_table where source = 'Twitter Web Client' group by place_type order by 2 desc;

SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / pi()) AS radius
  into hashtags_st_clusterwithin_01
FROM (
  SELECT unnest(ST_ClusterWithin(st_centroid(geom), 0.1)) gc
  FROM cte --where country_code = 'FR'
) f;

--alter table hashtags_regression_table add geom_centroid geometry;
--update hashtags_regression_table set geom_centroid = st_centroid(geom);
--create index ix_hashtags_regression_table_geom_centroid on hashtags_regression_table using gist(geom_centroid);
--alter table hashtags_regression_table add cluster_id int;
--create index ix_hashtags_regression_table_useful_geom_centroid on hashtags_regression_table_useful using gist(geom_centroid);
drop table if exists hashtags_st_clusterwithin_01_geom_dump;
select id,st_numgeometries, st_centroid((ST_Dump(geom_collection)).geom) as geom into hashtags_st_clusterwithin_01_geom_dump from hashtags_st_clusterwithin_01;
create index ix_hashtags_st_clusterwithin_01_geom_dump_geom on hashtags_st_clusterwithin_01_geom_dump using gist(geom);
update hashtags_regression_table_useful a set cluster_id = null;
set enable_seqscan=false;
update hashtags_regression_table_useful a set cluster_id = id from hashtags_st_clusterwithin_01_geom_dump c where c.geom=a.geom_centroid;
--alter table hashtags_regression_table add cluster_centroid geometry;
update hashtags_regression_table_useful a set cluster_centroid = centroid from hashtags_st_clusterwithin_01 c where c.id=a.cluster_id;
/*---------------------------------------------------VERY COOL FUNCTION FOR DISTANCE BASED CLUSTERING------------------------------------------------------------*/
select * from hashtags_st_clusterwithin_01 limit 10;
select distinct cluster_id, country,place_type,place_name from hashtags_regression_table_useful where cluster_id is not null and cluster_id = 309 order by 1,2,3 asc;

select id,st_numgeometries,st_astext(geom) from hashtags_st_clusterwithin_01_geom_dump limit 10;
select distinct id,st_numgeometries--, (ST_Dump(geom_collection)).geom
from hashtags_st_clusterwithin_01
where st_numgeometries>20;

select cluster_id,source,sum(tweets_count),sum(hashtags_count) from hashtags_regression_table_useful 
group by cluster_id,source order by sum(hashtags_count) desc;

select * from hashtags_regression_table_useful where cluster_id is not null limit 10;
/*---------------------------------------------------VERY COOL FUNCTION FOR DISTANCE BASED CLUSTERING------------------------------------------------------------*/
alter table hashtags_regression_table_useful add cluster_rank int;
update hashtags_regression_table_useful c1 set cluster_rank = sub.rank from (select country_code,cluster_id,sum(hashtags_count),row_number() over (partition by country_code,hashtag order by sum(hashtags_count) desc) as rank
											          from hashtags_regression_table_useful c2 
											          where  cluster_id is not null and country_code = 'FR'
											          group by country_code,cluster_id,hashtag) as sub where sub.country_code=c1.country_code and sub.cluster_id = c1.cluster_id;
select distinct country,cluster_id, cluster_rank from hashtags_regression_table_useful where country_code = 'ES' order by cluster_rank asc;
alter table hashtags_regression_table_useful add d_from_paris_geodesic_km int;
update hashtags_regression_table_useful a set d_from_paris_geodesic_km = (st_distance_sphere(st_makepoint(2.3508, 48.8567),b.cluster_centroid)/1000)::int
													   from (select distinct cluster_centroid,cluster_id, country_code from hashtags_regression_table_useful where cluster_rank=1) b 
													   where b.country_code=a.country_code and a.cluster_id is not null;
------------CHECK:
select * from hashtags_regression_table_useful where place_name = 'Miami';
alter table hashtags_regression_table_useful add d_from_first_ranked_geodesic_km int;
update hashtags_regression_table_useful a set d_from_first_ranked_geodesic_km = (st_distance_sphere(a.cluster_centroid,b.cluster_centroid)/1000)::int
													   from (select distinct cluster_centroid,cluster_id, country_code from hashtags_regression_table_useful where cluster_rank=1) b 
													   where b.country_code=a.country_code;
------CHECK:
select distinct place_name, d_from_paris_geodesic_km, d_from_paris_geodesic_km,d_from_first_ranked_geodesic_km from hashtags_regression_table_useful where country_code = 'US' and cluster_rank is not null order by 1 asc;
------ALL CORRECT!
alter table hashtags_regression_table_useful add cum_count_t_1 int;
alter table hashtags_regression_table_useful add cum_count_rank_1_t_1 int;
alter table hashtags_regression_table_useful add cum_count_t_1_all_tweets int;
/*---------------------------------------CUMULATIVE FOR T MINUS 1--------------------------------------------*/
with cte as (
select country_code,cluster_id,hashtag, hour, hashtags_count, sum(hashtags_count) over(partition by country_code, hashtag, cluster_id order by hour asc)as sum
													from hashtags_regression_table_useful where cluster_id is not null order by country_code,cluster_id,hashtag,hour asc
													)
--select country_code,cluster_id,hashtag,hashtags_count,sum,coalesce(lag(c.sum) over(partition by c.country_code,c.cluster_id,hashtag order by c.hour asc),0) as cum_count_t_1 from cte c order by country_code,cluster_id,hashtag,hour asc												
update hashtags_regression_table_useful a set cum_count_t_1 = c1.cum_count_t_1 from (select country_code,cluster_id,hashtag,hour,coalesce(lag(c.sum) over(partition by c.country_code,c.cluster_id,hashtag order by c.hour asc),0) as cum_count_t_1
											    from cte c) as c1  where c1.country_code=a.country_code and a.cluster_id=c1.cluster_id and a.hashtag=c1.hashtag and a.hour=c1.hour;
--------CHECK:
select country_code,cluster_id,hashtag, hour, hashtags_count,cum_count_t_1 from hashtags_regression_table_useful where country_code = 'BR' order by country_code,cluster_id,hashtag,hour asc;
--------ALL CORRECT!
with cte as (
select country_code,cluster_id, hour, tweets_count,sum(tweets_count) over(partition by country_code, cluster_id order by hour asc)as sum
													from hashtags_regression_table_useful where cluster_id is not null order by country_code,cluster_id,hour asc)
update hashtags_regression_table_useful a set cum_count_t_1_all_tweets = c1.cum_count_t_1 from (select country_code,cluster_id,hour,coalesce(lag(c.sum) over(partition by c.country_code,c.cluster_id order by c.hour asc),0) as cum_count_t_1
											    from cte c) as c1  where c1.country_code=a.country_code and a.cluster_id=c1.cluster_id and a.hour=c1.hour;
											    
/*---------------------------------------CUMULATIVE FOR T MINUS 1--------------------------------------------*/
/*---------------------------------------CUMULATIVE FOR T MINUS 1 AND FIRST RANKED CITY--------------------------------------------*/
with cte as (
--drop table if exists hashtags_regression_table_cum_t_1_rank_1;
select country_code,hashtag, cluster_rank,cluster_id, hour, hashtags_count,cum_count_t_1,coalesce((select distinct cum_count_t_1
																    from hashtags_regression_table_useful h1 
																    where h1.country_code=h2.country_code and h1.hashtag=h2.hashtag and  h1.hour=h2.hour and h1.cluster_rank=1),0) 
																    as rank_1_cum_count_t_1
--into hashtags_regression_table_cum_t_1_rank_1
from hashtags_regression_table_useful h2 where country_code = 'FR' and cluster_rank is not null order by hashtag,hour,cluster_rank asc)
with c as(select country_code, hashtag, hour,cum_count_t_1 from hashtags_regression_table_useful where cluster_rank =1 group by country_code, hashtag, hour,cum_count_t_1)


update hashtags_regression_table_useful a set cum_count_rank_1_t_1 = c1.cum_count_t_1 from c as c1  
where c1.country_code=a.country_code and a.hashtag=c1.hashtag  and a.hour=c1.hour;
--------CHECK:
select hashtag, cluster_rank,cluster_id, hour, hashtags_count,cum_count_t_1,cum_count_rank_1_t_1
from hashtags_regression_table_useful h2 where country_code = 'BR' and cluster_rank is not null order by hashtag,hour,cluster_rank asc;
--------ALL CORRECT!											    
/*---------------------------------------CUMULATIVE FOR T MINUS 1 AND FIRST RANKED CITY--------------------------------------------*/

SELECT cluster_rank, cluster_id,hour,hashtags_count, cum_count_t_1, cum_count_rank_1_t_1 
FROM hashtags_regression_table_useful 
WHERE country_code = 'ES' and hashtag = 'PrayForParis'and cluster_id is not null
order by cluster_rank,hour asc;

/*---------------------------------------------------------------------------------------------------------REGRESSION FINAL------------------------------------------------------------------------------------------------------------------------*/
select country_code,case when cluster_rank 1 then 1 else 0 end as capital_binary,hour,tweets_count, hashtags_count, d_from_paris_geodesic_km, d_from_first_ranked_geodesic_km, cum_count_rank_1_t_1,cum_count_t_1_all_tweets 
from hashtags_regression_table_useful where cluster_id is not null;
/*---------------------------------------------------------------------------------------------------------REGRESSION FINAL------------------------------------------------------------------------------------------------------------------------*/

select t.country,t.country_code,t.place_name,t.place_type,t.source,t.hour, h.hashtag, sum(t.count) as tweets_count, sum(h.count) as hashtags_count
select t.country,t.country_code,t.place_name,t.source,t.hour, h.hashtag, sum(t.count) as tweets_count, sum(h.count) as hashtags_count
--into hashtags_regression_table
from hashtgas_places_tweets_counts t left join hashtgas_paris_places_counts h on (h.country_code=t.country_code and h.place_type=t.place_type and h.place_name=t.place_name and h.hour=t.hour and h.source=t.source)
group by t.country,t.country_code,t.place_name,t.place_type,t.source,t.hour, h.hashtag
--order by 1,2,3,4,5,6,7 asc
limit 10;

select hour,hashtag,sum(tweets_count),sum(hashtags_count) 
from hashtags_regression_table
group by hour,hashtag
order by 1 asc;

create index ix_hashtags_regression_table_cluster_id on hashtags_regression_table using btree(cluster_id);
create index ix_hashtags_regression_table_hashtag on hashtags_regression_table using btree(hashtag);
create index ix_hashtags_regression_table_hour on hashtags_regression_table using btree(hour);

set enable_seqscan = FALSE;
--INSERT INTO tbl (thedate)
SELECT x.thedate
FROM (
   SELECT  cluster_id,hashtag,generate_series(min(hour), max(hour), '1h')::timestamp AS thedate
   FROM   hashtags_regression_table where cluster_id is not null group by cluster_id,hashtag
   ) x
WHERE NOT EXISTS (SELECT 1 FROM hashtags_regression_table t WHERE t.hour = x.thedate and t.hashtag = x.hashtag and t.cluster_id=x.cluster_id)

select * from hashtags_regression_table_useful limit 10;

SELECT  cluster_id,hashtag,generate_series(min(hour), max(hour), '1h')::timestamp AS date_hour,geom,geom_centroid,cluster_rank,d_from_paris_geodesic_km,d_from_first_ranked_geodesic_km,
	sum(tweets_count) as tweets_count,sum(hashtags_count) as hashtags_count
   FROM   hashtags_regression_table_useful 
   where cluster_id is not null and cluster_id is not null
   group by cluster_id,hashtag,generate_series(min(hour), max(hour), '1h')::timestamp AS date_hour,geom,geom_centroid,cluster_rank,d_from_paris_geodesic_km,d_from_first_ranked_geodesic_km
   order by cluster_id, generate_series(min(hour), max(hour), '1h')::timestamp,hashtag asc;

alter table hashtags_regression_table_useful add id serial;
delete from hashtags_regression_table_useful where cluster_id is null;
select max(id) from hashtags_regression_table_useful; -- 112.853
delete from hashtags_regression_table_useful where id > 112853;

 set enable_seqscan = FALSE;
INSERT INTO hashtags_regression_table_useful (country,country_code,geom,geom_centroid,cluster_centroid,cluster_rank,source,place_name,place_type,cluster_id,hashtag,hour,tweets_count,hashtags_count)
SELECT x.*,0,0
FROM (
   SELECT  country,country_code,geom,geom_centroid,cluster_centroid,cluster_rank,source,place_name,place_type,cluster_id,hashtag,generate_series(min(hour), max(hour), '1h')::timestamp AS thedate
   FROM   hashtags_regression_table_useful where cluster_id is not null group by country,country_code,place_name,place_type,cluster_id,hashtag,source,geom,geom_centroid,cluster_centroid,cluster_rank
   ) x
WHERE NOT EXISTS (SELECT 1 FROM hashtags_regression_table_useful t WHERE t.hour = x.thedate and t.hashtag = x.hashtag and t.cluster_id=x.cluster_id and t.country = x.country and t.country_code = x.country_code and 
				t.source = x.source and t.place_name = x.place_name and t.place_type = x.place_type);

select * from hashtags_regression_table_useful where tweets_count = 0 limit 10;
---sume moraju biti na nivou clustera a ne place_name jer su rangovi na nivou clustera
select * from hashtags_regression_table_useful where tweets_count = 0 limit 10;

--drop table hashtags_regression_table_useful_final;

select country_code,cluster_id,cluster_rank,hour, cluster_centroid, hashtag,sum(tweets_count) as tweets_count
	,sum(hashtags_count)  as hashtags_count
	into cte
from hashtags_regression_table_useful
where cluster_rank is not null
group by country_code, cluster_rank, cluster_id,hour,hashtag, cluster_centroid
order by hour,country_code, cluster_rank, cluster_id,hashtag asc

create index ix_cte_country_code on cte using btree(country_code);
create index ix_cte_cluster_rank on cte using btree(cluster_rank);
create index ix_cte_cluster_id on cte using btree(cluster_id);
create index ix_cte_hour on cte using btree(hour);
create index ix_cte_cluster_centroid on cte using gist(cluster_centroid);

set enable_seqscan=FALSE;
select *,sum(tweets_count) over(partition by country_code,cluster_rank,cluster_id,hashtag order by hour asc) as cum_tweets
	,sum(hashtags_count) over(partition by country_code,cluster_rank,cluster_id,hashtag order by hour asc) as cum_hashtags
	,coalesce((select sum(tweets_count) over(partition by country_code,cluster_rank,cluster_id order by hour asc) as cum_tweets
	 from cte c1 where c1.country_code = c2.country_code and c1.hour=c2.hour and c1.cluster_rank =1 limit 1
	 ),0) as delta_tweets_capital
	 ,coalesce((select sum(hashtags_count) over(partition by country_code,cluster_rank,cluster_id,hashtag order by hour asc) as cum_hashtags
	   from cte c1 where c1.country_code = c2.country_code and c1.hashtag=c2.hashtag and c1.hour=c2.hour and c1.cluster_rank =1
	 ),0) as delta_hashtags_capital  --coalesce in case cluster #1 has no hashtas for that hour	 
into hashtags_regression_table_useful_final
from cte c2
order by country_code,hour, cluster_rank asc;
--drop table hashtags_regression_table_useful_final;

alter table hashtags_regression_table_useful_final add d_paris_km int;
update hashtags_regression_table_useful_final a set d_paris_km = (st_distance_sphere(st_makepoint(2.3508, 48.8567),b.cluster_centroid)/1000)::int
													   from (select distinct cluster_centroid,cluster_id, country_code from hashtags_regression_table_useful_final where cluster_rank=1) b 
													   where b.country_code=a.country_code and a.cluster_id=b.cluster_id;
------------CHECK:
select *,((extract(day from (hour::timestamp-'2015-11-13 21:00:00'::timestamp)))::int)*24+
                     ((extract(hour from (hour::timestamp-'2015-11-13 21:00:00'::timestamp)))::int)
                     as hours_since from hashtags_regression_table_useful_final where cluster_id = 274 and hashtag = 'PrayForParis';
--CORRECT!
alter table hashtags_regression_table_useful_final add d_capital_km int;
update hashtags_regression_table_useful_final a set d_capital_km = (st_distance_sphere(a.cluster_centroid,b.cluster_centroid)/1000)::int
													   from (select distinct cluster_centroid,cluster_id, country_code from hashtags_regression_table_useful_final where cluster_rank=1) b 
													   where b.country_code=a.country_code;
------CHECK:
select distinct place_name, d_from_paris_geodesic_km, d_from_paris_geodesic_km,d_from_first_ranked_geodesic_km from hashtags_regression_table_useful where country_code = 'US' and cluster_rank is not null order by 1 asc;
alter table hashtags_regression_table_useful_final rename delta_hashtags_caplital to delta_hashtags_capital;

select count(*) from hashtags_regression_table_useful_final where tweets_count = 0 limit 10;

create index ix_hashtags_regression_table_useful_final_country_code on hashtags_regression_table_useful_final using btree(country_code);

select hashtag, hour, sum(hashtags_count) from hashtags_regression_table_useful_final group by hashtag,hour order by 1,2 asc;

select country_code,country from hashtags_regression_table
where country_code in ('US','PL','PH','PE','MY','LV','LT','JP','IT','EC','ES','GB','ID','CZ','CA','AT','AU','BR')
group by country_code,country order by 1 asc;

select * from tz_world limit 10;
select * from hashtags_st_clusterwithin_01 where id = 1233 limit 10;

select *      from hashtags_regression_table_useful_final 
                    where country_code in ('US','PL','PH','PE','MY','JP','IT','EC','ES','GB','ID','CZ','CA','AT','AU','BR')
                    order by country_code,cluster_rank,hashtag,hour asc limit 10;

select cluster_rank,sum(hashtags_count)
from hashtags_regression_table_useful_final where country_code = 'CA'
group by cluster_rank;


select cluster_id,sum(hashtags_count) from hashtags_regression_table_useful_final group by cluster_id order by 2 desc;
select country_code,sum(hashtags_count) from hashtags_regression_table_useful_final group by country_code order by 2 desc;
select * from hashtags_regression_table_useful_final limit 10;

alter table hashtags_regression_table_useful_final add cum_tweets_lag1 int;
alter table hashtags_regression_table_useful_final add cum_hashtags_lag1 int;

alter table hashtags_regression_table_useful_final add cum_tweets_lag2 int;
alter table hashtags_regression_table_useful_final add cum_hashtags_lag2 int;

alter table hashtags_regression_table_useful_final add h int;
alter table hashtags_regression_table_useful_final add h3 int;
alter table hashtags_regression_table_useful_final add h6 int;
update hashtags_regression_table_useful_final h set cum_tweets_lag1 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h
												 ,coalesce(lag(cum_tweets) over(partition by cluster_id,hashtag order by hour asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h=h.h;

update hashtags_regression_table_useful_final h set cum_hashtags_lag1 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h
												     ,coalesce(lag(cum_hashtags) over(partition by cluster_id,hashtag order by hour asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h=h.h;

update hashtags_regression_table_useful_final h set cum_tweets_lag2 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h
												 ,coalesce(lag(cum_tweets,2) over(partition by cluster_id,hashtag order by hour asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h=h.h;

update hashtags_regression_table_useful_final h set cum_hashtags_lag2 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h
												     ,coalesce(lag(cum_hashtags,2) over(partition by cluster_id,hashtag order by hour asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h=h.h;

update hashtags_regression_table_useful_final h set h =((extract(day from (hour::timestamp-'2015-11-13 21:00:00'::timestamp)))::int)*24+
													((extract(hour from (hour::timestamp-'2015-11-13 21:00:00'::timestamp)))::int);
update hashtags_regression_table_useful_final h set h3 = h/3;
update hashtags_regression_table_useful_final h set h6 = h/6;

alter table hashtags_regression_table_useful_final add h int;
alter table hashtags_regression_table_useful_final add h3 int;
alter table hashtags_regression_table_useful_final add h6 int;

select distinct h,h3,h6 
from hashtags_regression_table_useful_final order by h asc;

select country_code,cluster_id,hashtag,h,cum_tweets,coalesce(lag(cum_tweets) over(partition by cluster_id,hashtag order by hour asc),0) as lag_cum_tweets,cum_tweets_lag1,h3_hashtag_count 
from hashtags_regression_table_useful_final h1 where country_code = 'IT' and hashtag = 'PrayForParis' order by cluster_id,h asc;
select cum_hashtags,cum_hashtags_lag1,cum_tweets,cum_tweets_lag1 from hashtags_regression_table_useful_final where country_code = 'IT' and hashtag = 'PrayForParis' order by cluster_id,h asc;

alter table hashtags_regression_table_useful_final add h3_hashtag_count int;
update hashtags_regression_table_useful_final a set h3_hashtag_count = b.cnt from (select country_code,cluster_rank,cluster_id,hashtag,h3,sum(hashtags_count) as cnt
														        from hashtags_regression_table_useful_final group by country_code,cluster_rank,cluster_id,hashtag,h3 order by h3 asc) b 
														        where a.country_code=b.country_code and a.cluster_rank=b.cluster_rank and a.cluster_id=b.cluster_id and a.hashtag=b.hashtag and a.h3=b.h3;
--CHECK:
select country_code,cluster_rank,hashtag,h,h3,hashtags_count, h3_hashtag_count from hashtags_regression_table_useful_final where country_code = 'IT' and hashtag = 'PrayForParis' order by cluster_rank,h,h3 asc;
----Correct!
alter table hashtags_regression_table_useful_final add h3_tweets_count int;
update hashtags_regression_table_useful_final a set h3_tweets_count = b.cnt from (select country_code,cluster_rank,cluster_id,h3,sum(tweets_count) as cnt
														        from hashtags_regression_table_useful_final group by country_code,cluster_rank,cluster_id,h3 order by h3 asc) b 
														        where a.country_code=b.country_code and a.cluster_rank=b.cluster_rank and a.cluster_id=b.cluster_id and a.h3=b.h3;
--CHECK:
select country_code,cluster_rank,hashtag,h,h3,tweets_count, h3_tweets_count from hashtags_regression_table_useful_final where country_code = 'IT' and h3 < 4 order by cluster_rank,h,h3 asc;
----Correct!
alter table hashtags_regression_table_useful_final add h3_hashtags_cumulative int;
with cte as(select cluster_id,hashtag,h3,array_agg(hashtags_count) over (partition by cluster_id,hashtag order by h3 asc) as cnt
														        from hashtags_regression_table_useful_final group by country_code,cluster_rank,cluster_id,hashtag,h3,hashtags_count 
														        order by cluster_id,hashtag,h3 asc)
														        select * from cte;
update hashtags_regression_table_useful_final a set h3_hashtags_cumulative = b.cnt from (select cluster_id,hashtag,h3,sum(hashtags_count) over (partition by cluster_id,hashtag order by h3 asc) as cnt
														        from hashtags_regression_table_useful_final --group by country_code,cluster_rank,cluster_id,h3 
														        order by cluster_id,hashtag,h3 asc) b 
														        where a.country_code=b.country_code and a.cluster_rank=b.cluster_rank and a.cluster_id=b.cluster_id and a.h3=b.h3 and a.hashtag=b.hashtag;
--CHECK:
select country_code,cluster_rank,hashtag,h3,tweets_count, h3_tweets_count,h3_hashtags_cumulative 
from hashtags_regression_table_useful_final 
where country_code = 'IT' and h3 < 4 and hashtag='ParisAttacks'
order by cluster_rank,h3,hashtag asc;
----Correct!
select * from hashtags_regression_table_useful_final where country_code = 'IT' and cluster_rank = 1 order by h asc;
select country_code,cluster_rank,cluster_id,hashtag,h3,d_paris_km,d_capital_km
                     ,sum(tweets_count) as tweets_count
                     ,sum(hashtags_count) as hashtags_count
                     from hashtags_regression_table_useful_final 
                     where country_code ='IT' and hashtag='PrayForParis' 
                     group by h3,country_code,cluster_rank,cluster_id,hashtag,h3,d_paris_km,d_capital_km,case when cluster_rank=1 then 1 else 0 end--,hashtags_count
                     order by country_code,cluster_rank,hashtag, h3 asc;
select * from hashtags_regression_table_useful_final as t(x) where cluster_id = 359;

select distinct cluster_id,hashtag,h3,sum(hashtags_count) over w as cum_hashtags_count
	,array_agg(hashtags_count) over w as cnt
from hashtags_regression_table_useful_final as t(x) where cluster_id = 359
window w as (partition by cluster_id, hashtag order by h asc)
order by cluster_id,hashtag,h3 asc;

--drop table hashtags_regression_table_useful_final_h3;
select country_code,hashtag,cluster_id,cluster_rank,h3,cluster_centroid,sum(hashtags_count) as hashtags_count,sum(tweets_count) as tweets_count
into hashtags_regression_table_useful_final_h3
from hashtags_regression_table_useful_final --where cluster_id = 359
group by country_code,cluster_id,cluster_rank,h3,hashtag
order by country_code,cluster_id,hashtag,h3 asc;
----------new 3 hour aggregation tables:
select * from hashtags_regression_table_useful_final limit 10;
--drop table cte;
select country_code,cluster_id,cluster_rank,h3, cluster_centroid, hashtag,sum(tweets_count) as tweets_count,sum(hashtags_count)  as hashtags_count
	into cte
from hashtags_regression_table_useful_final
where cluster_rank is not null
group by country_code, cluster_rank, cluster_id,h3,hashtag, cluster_centroid
order by country_code,cluster_id,hashtag,h3 asc;

create index ix_cte_country_code on cte using btree(country_code);
create index ix_cte_cluster_rank on cte using btree(cluster_rank);
create index ix_cte_cluster_id on cte using btree(cluster_id);
create index ix_cte_h3 on cte using btree(h3);
create index ix_cte_cluster_centroid on cte using gist(cluster_centroid);

select * from hashtags_regression_table_useful_final where cluster_id = 1 and h3 = 1 and hashtag='ParisAttacks'; --Check: Correct!

set enable_seqscan=FALSE;
select *,sum(tweets_count) over(partition by country_code,cluster_rank,cluster_id,hashtag order by h3 asc) as cum_tweets
	,sum(hashtags_count) over(partition by country_code,cluster_rank,cluster_id,hashtag order by h3 asc) as cum_hashtags
	,coalesce((select sum(tweets_count) over(partition by country_code,cluster_rank,cluster_id order by h3 asc) as cum_tweets
	 from cte c1 where c1.country_code = c2.country_code and c1.h3=c2.h3 and c1.cluster_rank =1 limit 1
	 ),0) as delta_tweets_capital
	 ,coalesce((select sum(hashtags_count) over(partition by country_code,cluster_rank,cluster_id,hashtag order by h3 asc) as cum_hashtags
	   from cte c1 where c1.country_code = c2.country_code and c1.hashtag=c2.hashtag and c1.h3=c2.h3 and c1.cluster_rank =1
	 ),0) as delta_hashtags_capital  --coalesce in case cluster #1 has no hashtas for that hour	 
into hashtags_regression_table_useful_final_h3
from cte c2
order by country_code, cluster_rank, h3 asc;
--Query returned successfully: 118584 rows affected, 17:54 minutes execution time.
--drop table hashtags_regression_table_useful_final;

alter table hashtags_regression_table_useful_final_h3 add d_paris_km int;
update hashtags_regression_table_useful_final_h3 a set d_paris_km = (st_distance_sphere(st_makepoint(2.3508, 48.8567),b.cluster_centroid)/1000)::int
													   from (select distinct cluster_centroid,cluster_id, country_code from hashtags_regression_table_useful_final_h3 where cluster_rank=1) b 
													   where b.country_code=a.country_code;
------------CHECK:
select * from hashtags_regression_table_useful_final_h3 where cluster_id = 1234 limit 10; --correct!
select *,((extract(day from (h3::timestamp-'2015-11-13 21:00:00'::timestamp)))::int)*24+
                     ((extract(hour from (hour::timestamp-'2015-11-13 21:00:00'::timestamp)))::int)
                     as hours_since from hashtags_regression_table_useful_final_h3 where cluster_id = 274 and hashtag = 'PrayForParis';
--CORRECT!
alter table hashtags_regression_table_useful_final_h3 add d_capital_km int;
update hashtags_regression_table_useful_final_h3 a set d_capital_km = (st_distance_sphere(a.cluster_centroid,b.cluster_centroid)/1000)::int
													   from (select distinct cluster_centroid,cluster_id, country_code from hashtags_regression_table_useful_final_h3 where cluster_rank=1) b 
													   where b.country_code=a.country_code;
------CHECK:
select distinct cluster_id, d_paris_km, d_capital_km from hashtags_regression_table_useful_final_h3 where country_code = 'US' and cluster_rank is not null order by 1 asc; ---Correct!
alter table hashtags_regression_table_useful_final_h3 rename delta_hashtags_caplital to delta_hashtags_capital;

alter table hashtags_regression_table_useful_final_h3 add cum_tweets_lag1 int;
alter table hashtags_regression_table_useful_final_h3 add cum_hashtags_lag1 int;

alter table hashtags_regression_table_useful_final_h3 add cum_tweets_lag2 int;
alter table hashtags_regression_table_useful_final_h3 add cum_hashtags_lag2 int;

update hashtags_regression_table_useful_final_h3 h set cum_tweets_lag1 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h3
												 ,coalesce(lag(cum_tweets) over(partition by cluster_id,hashtag order by h3 asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final_h3 h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h3=h.h3;

update hashtags_regression_table_useful_final_h3 h set cum_hashtags_lag1 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h3
												     ,coalesce(lag(cum_hashtags) over(partition by cluster_id,hashtag order by h3 asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final_h3 h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h3=h.h3;

update hashtags_regression_table_useful_final_h3 h set cum_tweets_lag2 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h3
												 ,coalesce(lag(cum_tweets,2) over(partition by cluster_id,hashtag order by h3 asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final_h3 h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h3=h.h3;

update hashtags_regression_table_useful_final_h3 h set cum_hashtags_lag2 =h2.lag_cum_tweets from (select country_code,cluster_id,hashtag,h3
												     ,coalesce(lag(cum_hashtags,2) over(partition by cluster_id,hashtag order by h3 asc),0) as lag_cum_tweets
												  from hashtags_regression_table_useful_final_h3 h1) h2
												  where h2.cluster_id=h.cluster_id and h2.hashtag=h.hashtag and h2.h3=h.h3;

select * from hashtags_regression_table_useful_final_h3 where country_code = 'IT' and hashtag='PrayForParis' and delta_hashtags_capital > 0 limit 10;
--weird Cook's distance cluster_ids:
select distinct place_name from hashtags_regression_table_useful where cluster_id = 1623;
select distinct cluster_rank,cluster_id,place_name  from hashtags_regression_table_useful where country_code='US' order by 1 asc;

alter table hashtags_regression_table_useful_final_h3 add continent varchar(20);
alter table hashtags_regression_table_useful_final_h3 alter continent type varchar(25);
update hashtags_regression_table_useful_final_h3 set continent = case when country_code in ('US','CA','BR','EC','PE') then '02_Americas'
												     when country_code in ('FR','ES','IT','GB','PL','LT','CZ','AT') then '01_Europe'
												     when country_code in ('ID','JP','AU','MY','PH') then '03_Australia and Asia'
												     else 'else' end;
												     
												     select * from hashtags_regression_table_useful_final_h3 where continent = 'Americas' limit 10;
--VISUALIZATION:
select * into hashtags_PrayForParis_03_h from hashtags_paris_world where time_windows_6_h <4 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_06_h from hashtags_paris_world where time_windows_6_h >3 and time_windows_6_h < 7 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_09_h from hashtags_paris_world where time_windows_6_h >6 and time_windows_6_h < 10 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_12_h from hashtags_paris_world where time_windows_6_h >9 and time_windows_6_h < 13 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_15_h from hashtags_paris_world where time_windows_6_h >12 and time_windows_6_h < 16 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_18_h from hashtags_paris_world where time_windows_6_h >15 and time_windows_6_h < 19 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_21_h from hashtags_paris_world where time_windows_6_h >18 and time_windows_6_h < 22 and hashtag='PrayForParis';
select * into hashtags_PrayForParis_24_h from hashtags_paris_world where time_windows_6_h >21 and time_windows_6_h < 25 and hashtag='PrayForParis';

select * into hashtags_ParisAttacks_03_h from hashtags_paris_world where time_windows_6_h <4 and hashtag='PrayForParis';
select * into hashtags_ParisAttacks_06_h from hashtags_paris_world where time_windows_6_h >3 and time_windows_6_h < 7 and hashtag='ParisAttacks';
select * into hashtags_ParisAttacks_09_h from hashtags_paris_world where time_windows_6_h >6 and time_windows_6_h < 10 and hashtag='ParisAttacks';
select * into hashtags_ParisAttacks_12_h from hashtags_paris_world where time_windows_6_h >9 and time_windows_6_h < 13 and hashtag='ParisAttacks';
select * into hashtags_ParisAttacks_15_h from hashtags_paris_world where time_windows_6_h >12 and time_windows_6_h < 16 and hashtag='ParisAttacks';
select * into hashtags_ParisAttacks_18_h from hashtags_paris_world where time_windows_6_h >15 and time_windows_6_h < 19 and hashtag='ParisAttacks';
select * into hashtags_ParisAttacks_21_h from hashtags_paris_world where time_windows_6_h >18 and time_windows_6_h < 22 and hashtag='ParisAttacks';
select * into hashtags_ParisAttacks_24_h from hashtags_paris_world where time_windows_6_h >21 and time_windows_6_h < 25 and hashtag='ParisAttacks';


select * into hashtags_fusillade_03_h from hashtags_paris_world where time_windows_6_h <4 and hashtag='fusillade';
select * into hashtags_fusillade_06_h from hashtags_paris_world where time_windows_6_h >3 and time_windows_6_h < 7 and hashtag='fusillade';
select * into hashtags_fusillade_09_h from hashtags_paris_world where time_windows_6_h >6 and time_windows_6_h < 10 and hashtag='fusillade';
select * into hashtags_fusillade_12_h from hashtags_paris_world where time_windows_6_h >9 and time_windows_6_h < 13 and hashtag='fusillade';
select * into hashtags_fusillade_15_h from hashtags_paris_world where time_windows_6_h >12 and time_windows_6_h < 16 and hashtag='fusillade';
select * into hashtags_fusillade_18_h from hashtags_paris_world where time_windows_6_h >15 and time_windows_6_h < 19 and hashtag='fusillade';
select * into hashtags_fusillade_21_h from hashtags_paris_world where time_windows_6_h >18 and time_windows_6_h < 22 and hashtag='fusillade';
select * into hashtags_fusillade_24_h from hashtags_paris_world where time_windows_6_h >21 and time_windows_6_h < 25 and hashtag='fusillade';

select * from hashtags_regression_table_useful_final_h3 where cluster_id = 359 order by hashtag,h3 asc;

select continent,hashtag,sum(hashtags_count)
from hashtags_regression_table_useful_final_h3
group by continent,hashtag
order by 1,2 asc;

select * from hashtags_regression_table_useful_final_h3 limit 10;
alter table hashtags_regression_table_useful_final_h3 add nn_higher_rank_clust_id int;

create index ix_hashtags_regression_table_useful_final_h3_cluster_centroid on hashtags_regression_table_useful_final_h3 using gist(cluster_centroid);
create index ix_hashtags_regression_table_useful_final_h3_cluster_rank on hashtags_regression_table_useful_final_h3 using btree(cluster_rank);
create index ix_hashtags_regression_table_useful_final_h3_cluster_id on hashtags_regression_table_useful_final_h3 using btree(cluster_id);
create index ix_hashtags_regression_table_useful_final_h3_country_code on hashtags_regression_table_useful_final_h3 using btree(country_code);

set enable_seqscan = FALSE;
select distinct cluster_id,(select distinct h2.cluster_id 
			   from hashtags_regression_table_useful_final_h3 h2 
			   where h1.country_code=h2.country_code and h1.cluster_rank<h2.cluster_rank 
			   order by st_distance_sphere(h1.cluster_centroid,h2.cluster_centroid) limit 1)
from hashtags_regression_table_useful_final_h3 h1
where cluster_id = 1377;

select h1.cluster_id, h1.cluster_rank, h2.cluster_id, h2.cluster_rank,st_distance_sphere(h1.cluster_centroid,h2.cluster_centroid)
from hashtags_regression_table_useful_final_h3 h1,hashtags_regression_table_useful_final_h3 h2
where h1.cluster_id = 1448 and h1.country_code=h2.country_code and h1.cluster_rank>h2.cluster_rank
group by h1.cluster_id, h1.cluster_rank, h2.cluster_id, h2.cluster_rank,h1.cluster_centroid,h2.cluster_centroid
order by st_distance_sphere(h1.cluster_centroid,h2.cluster_centroid) asc limit 1;
---NEAREST NEIGHBOR WITH HIGHER RANK
update hashtags_regression_table_useful_final_h3 h set nn_higher_rank_clust_id = r. h2_cluster_id 
from (select h1.cluster_id as h1_cluster_id, h2.cluster_id as h2_cluster_id
	  from hashtags_regression_table_useful_final_h3 h1,hashtags_regression_table_useful_final_h3 h2
	  where h1.country_code=h2.country_code and h1.cluster_rank>h2.cluster_rank
	  group by h1.cluster_id, h1.cluster_rank, h2.cluster_id, h2.cluster_rank,h1.cluster_centroid,h2.cluster_centroid
	  order by st_distance_sphere(h1.cluster_centroid,h2.cluster_centroid) asc limit 1) r
	  where h.country_code='US' and r.h1_cluster_id=h.cluster_id
--where h.cluster_id = 1448;
--Query returned successfully: 118584 rows affected, 45.4 secs execution time.
--CHECK:
select * from hashtags_regression_table_useful_final_h3 where cluster_id = 1647; --Correct!

														     select * from hashtags_regression_table_useful_final_h3 where cluster_id = 1389;

select * from hashtags_regression_table_useful_final_h3 where cluster_rank>1 and delta_h_nn_higher_rank is null limit 10;

select distinct place_name from hashtags_regression_table_useful where cluster_id = 1377;
select distinct cluster_rank,cluster_id,place_name  from hashtags_regression_table_useful where country_code='US' order by 1 asc;

update hashtags_regression_table_useful_final_h3 c1 set nn_higher_rank_clust_id = (select c2.cluster_id from hashtags_regression_table_useful_final_h3 c2 where c1.country_code=c2.country_code 
															and c1.cluster_rank>c2.cluster_rank ORDER BY ST_Distance_sphere(c1.cluster_centroid,c2.cluster_centroid) limit 1);
set enable_seqscan=FALSE;

select h1.cluster_id as h1_cluster_id, h1.cluster_rank as h1_cluster_rank, h2.cluster_id as h2_cluster_id, h2.cluster_rank as h2_cluster_rank, st_distance_sphere(h1.cluster_centroid,h2.cluster_centroid) as d
into hashtags_nn_ranks
from hashtags_regression_table_useful_final_h3 h1,hashtags_regression_table_useful_final_h3 h2
where h1.country_code=h2.country_code
group by h1.cluster_id, h1.cluster_rank, h2.cluster_id, h2.cluster_rank,h1.cluster_centroid,h2.cluster_centroid
order by st_distance_sphere(h1.cluster_centroid,h2.cluster_centroid) asc;

--Query returned successfully: 703894 rows affected, 05:50:18034 hours execution time.
select * from hashtags_nn_ranks where h1_cluster_id=652;
create index ix_hashtags_nn_ranks_h1_cluster_id on hashtags_nn_ranks using btree(h1_cluster_id);
create index ix_hashtags_nn_ranks_h2_cluster_id on hashtags_nn_ranks using btree(h2_cluster_id);
create index ix_hashtags_nn_ranks_h1_cluster_rank on hashtags_nn_ranks using btree(h1_cluster_rank);
create index ix_hashtags_nn_ranks_h2_cluster_rank on hashtags_nn_ranks using btree(h2_cluster_rank);
create index ix_hashtags_nn_ranks_d on hashtags_nn_ranks using btree(d);

select * from hashtags_nn_ranks where h1_cluster_id = 1234;

alter table hashtags_regression_table_useful_final_h3 add nn_higher_rank_clust_id_1_5 int;
update hashtags_regression_table_useful_final_h3 c1 set nn_higher_rank_clust_id = (select h2_cluster_id from hashtags_nn_ranks c2 where c1.cluster_id=c2.h1_cluster_id 
															and c2.h1_cluster_rank>c2.h2_cluster_rank ORDER BY c2.d asc limit 1);
update hashtags_regression_table_useful_final_h3 c1 set nn_higher_rank_clust_id_1_5 = (select h2_cluster_id from hashtags_nn_ranks c2 where c1.cluster_id=c2.h1_cluster_id 
															and c2.h1_cluster_rank>c2.h2_cluster_rank and c2.h2_cluster_rank < 5 ORDER BY c2.d asc limit 1);
alter table hashtags_regression_table_useful_final_h3 add delta_h_nn_higher_rank_1_5 int;
update hashtags_regression_table_useful_final_h3 h1 set delta_h_nn_higher_rank_1_5 =0 where delta_h_nn_higher_rank is null;
update hashtags_regression_table_useful_final_h3 h1 set delta_h_nn_higher_rank_1_5 = coalesce(h2.cum_hashtags,0)
														           from hashtags_regression_table_useful_final_h3 h2 
														           where h1.nn_higher_rank_clust_id_1_5=h2.cluster_id 
														           and h1.h3=h2.h3 and h1.hashtag=h2.hashtag;
														     
															
select * from hashtags_regression_table_useful_final_h3 where cluster_id = 1234;
select distinct place_name from hashtags_regression_table_useful where cluster_id = 1623;
---NEAREST NEIGHBOR HIGHER RANK DELTAS:
alter table hashtags_regression_table_useful_final_h3 add delta_h_nn_higher_rank int;
update hashtags_regression_table_useful_final_h3 h1 set delta_h_nn_higher_rank =null;
update hashtags_regression_table_useful_final_h3 h1 set delta_h_nn_higher_rank = coalesce(h2.cum_hashtags,0)
														     from hashtags_regression_table_useful_final_h3 h2 
														     where h1.nn_higher_rank_clust_id=h2.cluster_id 
														     and h1.h3=h2.h3 and h1.hashtag=h2.hashtag;
update hashtags_regression_table_useful_final_h3 h1 set delta_h_nn_higher_rank =0 where delta_h_nn_higher_rank is null;
update hashtags_regression_table_useful_final_h3 h1 set delta_h_nn_higher_rank =coalesce(h2.cum_hashtags,0)
														     from hashtags_regression_table_useful_final_h3 h2 
														     where h1.delta_h_nn_higher_rank is null and h1.cluster_rank = 1 
														     and h2.cluster_id = 359
														     and h1.h3=h2.h3 and h1.hashtag=h2.hashtag;
select * from hashtags_regression_table_useful_final_h3 where delta_h_nn_higher_rank
select cluster_id,cluster_rank,h3,cum_hashtags,nn_higher_rank_clust_id,delta_h_nn_higher_rank
from hashtags_regression_table_useful_final_h3
where country_code = 'FR' and cluster_rank in (0,1,2,3,4) and hashtag = 'PrayForParis'
order by h3,cluster_rank,cluster_id asc;
SELECT * FROM hashtags_regression_table_useful_final_h3 WHERE COUNTRY_CODE = 'FR' AND delta_h_nn_higher_rank is null;
------CONTINENTS:
select country_code,sum(hashtags_count),sum(tweets_count),round(sum(hashtags_count)/sum(tweets_count),5)*100
from hashtags_regression_table_useful_final_h3
group by country_code
having sum(tweets_count) > 1000
order by 4 desc;

update hashtags_regression_table_useful_final_h3 set continent = case when country_code in ('CH','BE','DK','PL','FI','CZ','NO','RS','AT','DE','NL','SE','IT','GB','PT','IE','FR','ES') then '01_Europe'
													when country_code in ('GH','NG','ZA','KE') then '02_Africa'
													when country_code in ('US','CA') then '03_North_America'
													when country_code in ('BR','EC','PE','CL','VE','PY','CO','AR') then '04_South_America'
													when country_code in ('JP','PK','ID','IN','PH','RU','MY') then '05_Asia'
													when country_code in ('AU','NZ') then '06_Australia'
													when country_code in ('MX','DO') then '07_Central_America'
													when country_code in ('TR','SA') then '08_Middle_East'
													else '09_less_than_1k_tweets'
													end;
update hashtags_regression_table_useful_final_h3 set continent = case when country_code in ('CH','BE','DK','PL','FI','CZ','NO','RS','AT','DE','NL','SE','IT','GB','PT','IE','FR','ES','GH','NG','ZA','KE') then '01_Euro_tz'
													when country_code in ('US','CA','BR','EC','PE','CL','VE','PY','CO','AR','MX','DO') then '02_North_America_tz'
													when country_code in ('JP','PK','ID','IN','PH','RU','MY','AU','NZ') then '03_Asia_tz'
													when country_code in ('TR','SA') then '04_Middle_East_tz'
													else '05_less_than_1k_tweets'
													end;
select  country_code, cluster_rank,h3,sum(hashtags_count)
from hashtags_regression_table_useful_final_h3
where cluster_id in (1258,1389,1377,1224,1915,1448,1647,1767,1265,1907,1630,1298,1763) ---South Florida
group by country_code, cluster_rank,h3
order by country_code, cluster_rank,h3 asc;

SELECT tweet#>'{user,screen_name}', tweet#>'{text}',tweet
from paris_all where tweet#>>'{lang}' = 'en' and tweet#>>'{text}' ilike '%attack%' limit 10;

select * from paris_all_no_tweets limit 10;

	ALTER TABLE PARIS_ALL ADD FREQ_WORDS BOOLEAN;
	UPDATE PARIS_ALL SET FREQ_WORDS = TRUE WHERE INSERT_TIME > '2015-11-13 21:00:00.000000' AND INSERT_TIME < '2015-11-14 08:00:00.000000' AND TWEET#>>'{lang}'='en'
	and (
		TWEET#>>'{text}' ilike '% safe %' OR TWEET#>>'{text}' ilike '% pray %' OR TWEET#>>'{text}' ilike '% tonight %' OR TWEET#>>'{text}' ilike '% thoughts %' OR TWEET#>>'{text}' ilike '% attack%' OR
		TWEET#>>'{text}' ilike '% news %' OR TWEET#>>'{text}' ilike '% peace %' OR	TWEET#>>'{text}' ilike '% dead %' OR	TWEET#>>'{text}' ilike '% police %' OR	TWEET#>>'{text}' ilike '% happen%' OR
		TWEET#>>'{text}' ilike '% victim%' OR TWEET#>>'{text}' ilike '% bataclan %' OR TWEET#>>'{text}' ilike '% terror%' OR TWEET#>>'{text}' ilike '% horrible %' OR TWEET#>>'{text}' ilike '% tragedy %' OR
		TWEET#>>'{text}' ilike '% scared %' OR
		TWEET#>>'{text}' ilike '% stay % strong %'
		);
	ALTER TABLE PARIS_ALL_txt ADD FREQ_WORDS_CATEGORY VARCHAR;
	update PARIS_ALL_txt set FREQ_WORDS_CATEGORY = case when (text ilike '%attack%' OR text ilike '%dead%' or text ilike '%bataclan%' or text ilike '%scared%' or text ilike '%victim%'
												 or text ilike '%police%' or text ilike '%terror%' or text ilike '%tragedy%' or text ilike '%bataclan%' or text ilike '%emergency%')
												then 'events'
												when (text ilike '%pray%' OR text ilike '%thoughts%' OR text ilike '%heart%goes%' OR text ilike '%safe%' or text ilike '%stay%strong%')
													  then 'support'
													  else null end
													  where tweet_id not in (select tweet_id from hashtags_paris_world)
select * from PARIS_ALL_txt where FREQ_WORDS_CATEGORY is not null limit 10;
update paris_all p set FREQ_WORDS_CATEGORY = t.FREQ_WORDS_CATEGORY from paris_all_txt t where t.tweet_id=p.tweet_id;

	select tweet_id,insert_time,tweet#>>'{text}' as text
	into paris_all_txt
	from paris_all;

	create index ix_paris_all_txt_insert_time on paris_all_txt using btree(insert_time);

	select date_trunc('day', insert_time) as ddhh, hashtag,count(distinct h.tweet_id) as tweets, sum(urls) as urls, sum(user_mentions) as user_mentions
	from hashtags_paris_world h, paris_all_entities_counts e
	where h.tweet_id=e.tweet_id and h.hashtag in ('PrayForParis','ParisAttacks')
	group by date_trunc('day', insert_time), hashtag
	order by 1,2 asc;

	select date_trunc('day', insert_time) as dd, count(distinct h.tweet_id) as tweets
	from hashtags_paris_world h, paris_all p
	where p.tweet_id=h.tweet_id
	group by date_trunc('day', insert_time)
	order by 1 asc;

	select date_trunc('day', insert_time) as dd, count(distinct tweet_id) as tweets
	from paris_all
	group by date_trunc('day', insert_time)
	order by 1 asc;

	select date_trunc('day', insert_time) as dd,tweet#>>'{place,country_code}',count(id) 
	from ne_i 
	group by tweet#>>'{place,country_code}'
	order by 1 asc;

	select id,
	tweet#>>'{tweet_id}' as tweet_id,
	tweet#>>'{created_at}' as created_at, 
	tweet#>>'{place,country_code}' as cc, 
	tweet#>>'{place,full_name}' as place_name, 
	tweet#>>'{place,place_type}' as place_type,
	tweet#>>'{user,id}' as user_id
	into ne_i_filter
	from ne_i; 

	select t.FREQ_WORDS_CATEGORY, count(distinct t.tweet_id) 
	from paris_all_txt t, paris_all p where t.tweet_id=p.tweet_id
	group by t.FREQ_WORDS_CATEGORY

	select * from paris_all limit 10;
	create index ix_paris_all_journalist on paris_all using btree(journalist);
	
	select distinct tweet#>>'{user,screen_name}', tweet#>>'{user,description}' 
	from paris_all 
	where tweet_type is not null and
	tweet#>>'{user,screen_name}' 
	in ('ajkcnn','jeromegodefroy','Mathilde_Sd','shahidsiddiqui','BreakingLv','PaulMiquel','SanamF24','BBCChrisMorris','Reygje','MarieMihaileanu','Morinerie','Vince66240','Zagumennov_Tema','Politis_fr',
	'AbarnouThomas','clairesnews','elojociudadanoc','Drissabdi','austinkevin22','taimaz','PierreCourade','andpscott','niloofarpourebr','Adelhiniooo','MtiriHajer','WerthMathias','ColomboPostSin','paranoidgeeek',
	'whitneydawn','nikop17','michielwil','jessicaplautz','zeitonlinesport','chrysoline','quentinperinel','MBrancourt','mattijsvdwiel','TiffWertheimer9','Rafanelli','MarkusHoehner','Mel_ChaouchMena','GoSruthi','ldesbonnets',
	'ARobertsjourno','LeoNovel','LarsWallrodt','Exan_wuxu','JochenStutzky');

	update paris_all set journalist = null where tweet_type is not null;
	update paris_all set journalist = TRUE where tweet#>>'{user,screen_name}' 
	in ('ajkcnn','jeromegodefroy','Mathilde_Sd','shahidsiddiqui','BreakingLv','PaulMiquel','SanamF24','BBCChrisMorris','Reygje','MarieMihaileanu','Morinerie','Vince66240','Zagumennov_Tema','Politis_fr',
	'AbarnouThomas','clairesnews','elojociudadanoc','Drissabdi','austinkevin22','taimaz','PierreCourade','andpscott','niloofarpourebr','Adelhiniooo','MtiriHajer','WerthMathias','ColomboPostSin','paranoidgeeek',
	'whitneydawn','nikop17','michielwil','jessicaplautz','zeitonlinesport','chrysoline','quentinperinel','MBrancourt','mattijsvdwiel','TiffWertheimer9','Rafanelli','MarkusHoehner','Mel_ChaouchMena','GoSruthi','ldesbonnets',
	'ARobertsjourno','LeoNovel','LarsWallrodt','Exan_wuxu','JochenStutzky');

	select * from retweets_api limit 10;
	alter table retweets_api add journalist boolean;
	update retweets_api set journalist = TRUE where exists (select 1 from paris_all p where p.journalist = TRUE and p.tweet_id = retweets_api.tweet_id);

	create index ix_paris_all_txt on paris_all_txt using btree(tweet_id);
	create index ix_paris_all_freq_words_cat on paris_all_txt using btree(freq_words_category);
	set enable_seqscan = FALSE;
	update paris_all p set freq_words_category = t.freq_words_category from paris_all_txt t where t.tweet_id=p.tweet_id;
	update paris_all_txt t set retweets = p.retweets from paris_all p where p.tweet_id=t.tweet_id;
	update paris_all_txt t set favourites = p.favourites from paris_all p where p.tweet_id=t.tweet_id;

	update paris_all set journalist = FALSE where tweet_type = 'EVENTS' and journalist is null;
	select user_id,tweet_type, sum(retweets),sum(favourites),case when journalist is true then 1 else 0 end as journalista from paris_all where tweet_type = 'EVENTS' group by user_id,journalist,tweet_type;

	select * from paris_all_txt limit 10;
	select freq_words_category 
	from paris_all_txt where freq_words_category is null limit 30;

select  p.tweet_id, tweet_type, case when p.tweet_type is not null then 'Images'
						when p.hash_flag is not null
	from paris_all p, paris_all_txt t
	where p.tweet_id=t.tweet_id

------HASHTAG CATEGORY:
select t.medium,t.cat,count(distinct t.tweet_id),sum(retweets) as retweets,sum(favorites) as favorites
from
(
	SELECT tweet_id,CASE WHEN HASHTAG IN ('prayforparis','PrayForParis') then 'Support' else 'Events' end as cat, 'hashtag' as medium
,(select sum(p.retweets) from paris_all p where p.tweet_id=h.tweet_id) as retweets
,(select sum(p.favourites) from paris_all p where p.tweet_id=h.tweet_id) as favorites
FROM HASHTAGS_PARIS_all h where hashtag in ('prayforparis','ParisAttacks','PrayForParis','fusillade','AttentatsParis','SaintDenis','Bataclan') 
and tweet_id not in (select tweet_id from paris_all_txt where freq_words_category is not null)
and tweet_id not in (select tweet_id from paris_all where tweet_type is not null)
group by tweet_id,hashtag having count(distinct hashtag) =1
union
------KEYWORD CATEGORY:
(select tweet_id, freq_words_category as cat,'keyword' as medium, sum(retweets) as retweets, sum(favourites) as favorites from paris_all_txt 
where freq_words_category is not null and tweet_id not in (select distinct tweet_id from hashtags_paris_all)
group by tweet_id,freq_words_category 
having count(distinct freq_words_category) =1)
union
------PICTURE CATEGORY:
(select tweet_id,tweet_type as cat,'picture' as medium, sum(retweets) as retweets, sum(favourites) as favorites from paris_all where tweet_type is not null 
and tweet_id not in (select tweet_id from paris_all_txt where FREQ_WORDS_CATEGORY is not null) and hash_flag is null
group by tweet_id, tweet_type having count(distinct tweet_type)=1 order by 1 asc)
) t
group by t.cat,t.medium
order by t.medium,t.cat asc;


		 select t.freq_words_category,count(t.tweet_id),avg(p.retweets) as avg_retweets,avg(p.favourites) as avg_retweets
		 from PARIS_ALL_txt t, paris_all p
		 where t.FREQ_WORDS_CATEGORY is not null and t.tweet_id=p.tweet_id and p.retweets is not null and p.hash_flag is null and tweet_type is null
		 group by t.freq_words_category;

		 select tweet_type, avg(retweets), avg(favourites),count(distinct tweet_id)
		from paris_all
		where hash_flag is null and tweet_type is not null
		and tweet_id not in (select tweet_id from paris_all_txt where FREQ_WORDS_CATEGORY is not null)
		group by tweet_type;

select tweet#>>'{user,screen_name}',tweet_id,retweets from paris_all where retweets > 5 order by 3 desc limit 100;
select * from hashtags_paris_world limit 10;

select hashtag,count(distinct p.tweet_id),round(avg(p.retweets),2), round(avg(p.favourites),2)
from hashtags_paris_all h, paris_all_txt p
where hashtag in ('AttentatsParis','Bataclan','fusillade','ParisAttacks','PrayForParis','prayforparis','SaintDenis') and h.tweet_id=p.tweet_id
group by hashtag
order by 1 asc;

select * from PARIS_ALL_txt limit 10;
alter table hashtags_paris_world add favorites int;
alter table hashtags_paris_world add retweets int;
update hashtags_paris_world h set favorites = p.favourites from paris_all_txt p where p.tweet_id = h.tweet_id;
update hashtags_paris_world h set retweets = p.retweets from paris_all_txt p where p.tweet_id = h.tweet_id;

select count(*) from hashtags_paris_world where retweets is not null;
select user_id
                ,round(avg(retweets),2) as retweets
                ,round(avg(favourites),2) as favorites
                ,case when journalist is true then 1 
                 else 0 end 
                  as journalista 
                  from paris_all where tweet_type = 'EVENTS' 
                  group by user_id,journalist;

select count(distinct tweet_id) from retweets_api;
select  from retweets_api limit 10;
select extract(day from created_at), count(distinct tweet_id) from retweets_api where tweet_type is not null group by extract(day from created_at);


set enable_seqscan = FALSE;
--INSERT INTO tbl (thedate)
SELECT x.thedate
FROM (
   SELECT  cluster_id,hashtag,generate_series(min(h3), max(h3), '1') AS thedate
   FROM   hashtags_regression_table_useful_final_h3 where cluster_id is not null group by cluster_id,hashtag
   ) x
WHERE NOT EXISTS (SELECT 1 FROM hashtags_regression_table_useful_final_h3 t WHERE t.h3 = x.thedate and t.hashtag = x.hashtag and t.cluster_id=x.cluster_id)
--select * from hashtags_regression_table_useful_final_h3 limit 10;
set enable_seqscan = FALSE;
INSERT INTO hashtags_regression_table_useful_final_h3 (country,country_code,geom,geom_centroid,cluster_centroid,cluster_rank,source,place_name,place_type,cluster_id,hashtag,hour,tweets_count,hashtags_count)
SELECT x.*,0,0
FROM (
   SELECT  country,country_code,geom,geom_centroid,cluster_centroid,cluster_rank,source,place_name,place_type,cluster_id,hashtag,generate_series(min(hour), max(hour), '1h')::timestamp AS thedate
   FROM   hashtags_regression_table_useful_final_h3 where cluster_id is not null group by country,country_code,place_name,place_type,cluster_id,hashtag,source,geom,geom_centroid,cluster_centroid,cluster_rank
   ) x
WHERE NOT EXISTS (SELECT 1 FROM hashtags_regression_table_useful_final_h3 t WHERE t.h3 = x.thedate and t.hashtag = x.hashtag and t.cluster_id=x.cluster_id and t.country = x.country and t.country_code = x.country_code and 
				t.source = x.source and t.place_name = x.place_name and t.place_type = x.place_type);
select * from hashtags_regression_table_useful_final_h3 limit 10;

select country_code,h3,count(*) 
from hashtags_regression_table_useful_final_h3
where h3 < 15
group by country_code,h3
order by 1,2 asc;


select continent,cluster_id
                    ,case when cluster_rank=1 then 1 else 0 end as capital_binary
                    ,sum(cum_tweets_lag1) as cum_tweets_lag1,sum(cum_hashtags_lag1) as cum_hashtags_lag1
               ,h3
               ,sum(tweets_count) as tweets_counts
               ,sum(hashtags_count) as hashtags_count
               ,sum(cum_tweets) as cum_tweets
               ,sum(cum_hashtags) as cum_hashtags
               ,sum(delta_tweets_capital) as delta_tweets_capital
               ,sum(delta_hashtags_capital) as delta_hashtags_capital
               ,(avg(d_paris_km))::int as d_paris_km
               ,(avg(d_capital_km))::int as d_capital_km
                ,(avg(delta_h_nn_higher_rank))::int as delta_h_nn_higher_rank
                ,(avg(delta_h_nn_higher_rank_1_5))::INT as delta_h_nn_higher_rank_1_5
               from hashtags_regression_table_useful_final_h3
               where delta_h_nn_higher_rank is not null
               group by continent,cluster_id,case when cluster_rank=1 then 1 else 0 end,h3
               order by continent,cluster_id,h3 asc;

select cluster_id,h3,tweets_count,cum_tweets from hashtags_regression_table_useful_final_h3 where cluster_id = 1 order by 1,2 asc;
select cluster_id, h3, sum(tweets_count) as cnt from hashtags_regression_table_useful_final_h3 group by cluster_id, h3 order by 1,2;

select cluster_id,h3,tweets_count,cum_tweets, sum(tweets_count) over(partition by cluster_id order by h3 asc) as cum_tweets 
from newdat where cluster_id = 1 and h3 < 100 order by 1,2 asc;
select cluster_id,h3,hashtags_count,cum_hashtags, sum(hashtags_count) over(partition by cluster_id order by h3 asc) as cum_hashtags 
from newdat where cluster_id = 1 and h3 < 100 order by 1,2 asc;

update newdat set cum_tweets = 0;
with cte as (select cluster_id,h3,tweets_count, sum(tweets_count) over(partition by cluster_id order by h3 asc) as cum_tweets 
from hashtags_regression_table_useful_final_h3 order by 1,2 asc)

update newdat n set cum_tweets = cte.cum_tweets from cte where n.cluster_id = cte.cluster_id and n.h3 = cte.h3;

update newdat set cum_hashtags = 0;
with cte as (select cluster_id,h3,hashtags_count, sum(hashtags_count) over(partition by cluster_id order by h3 asc) as cum_hashtags 
from hashtags_regression_table_useful_final_h3 order by 1,2 asc)

update newdat n set cum_hashtags = cte.cum_hashtags from cte where n.cluster_id = cte.cluster_id and n.h3 = cte.h3;

select * from newdat order by cluster_id, h3 asc limit 100;
coalesce((select sum(tweets_count) over(partition by country_code,cluster_rank,cluster_id order by h3 asc) as cum_tweets

update newdat set cum_hashtags_lag1=0;
update newdat set cum_hashtags_lag1 = a.cum_hashtags_lag1 from (
select cluster_id, h3, cum_hashtags, cum_hashtags_lag1 as ab, coalesce(lag(cum_hashtags) over(partition by cluster_id order by h3 asc),0) as cum_hashtags_lag1 from newdat order by cluster_id, h3 asc
                  ) as a where a.cluster_id=newdat.cluster_id and a.h3=newdat.h3;

update newdat set cum_tweets_lag1=0;
update newdat set cum_tweets_lag1 = a.cum_tweets_lag1 from (
select cluster_id, h3, cum_tweets, cum_tweets_lag1 as ab, coalesce(lag(cum_tweets) over(partition by cluster_id order by h3 asc),0) as cum_tweets_lag1 from newdat order by cluster_id, h3 asc
                  ) as a where a.cluster_id=newdat.cluster_id and a.h3=newdat.h3;

select country_code, cluster_id, cluster_rank, h3,hashtags_count,delta_hashtags_capital from newdat where cluster_id < 100 and h3 < 3 order by country_code, cluster_rank, h3 asc limit 100;

select cluster_id,h3 from newdat group by cluster_id,h3 having count(*) > 1;
select * from newdat where cluster_id = 47 and h3 in (0,13);

select country_code, cluster_rank, cluster_id,h3, d_capital_km from newdat order by 1,2,3 asc;

select country_code, cluster_rank, cluster_id, d_capital_from_paris_km, d_from_paris,d_capital_km from newdat order by 1,2,3 asc;
update newdat set d_capital_km = d_from_first_ranked_geodesic_km from hashtags_regression_table_useful a where newdat.cluster_id = a.cluster_id;
													   where b.country_code=a.country_code and a.cluster_id is not null;
------------CHECK:
select * from hashtags_regression_table_useful where place_name = 'Miami';
select distinct place_name from hashtags_regression_table_useful where cluster_id = 554;
select * from hashtags_regression_table_useful_final_h3 limit 1;

alter table newdat add x numeric;
alter table newdat add y numeric;
update newdat set x = st_x(cluster_centroid) from hashtags_regression_table_useful_final_h3 a where a.cluster_id = newdat.cluster_id;
update newdat set y = st_y(cluster_centroid) from hashtags_regression_table_useful_final_h3 a where a.cluster_id = newdat.cluster_id;

copy(select * from newdat
                         where cluster_id in 
                         (select cluster_id from newdat group by cluster_id 
                         having sum(hashtags_count) > 80)) to '/tmp/newdat.csv' with CSV header delimiter ';';



select cluster_id, sum(tweets_count) as total_tweets_count, sum(hashtags_count) as total_hashtags_count
from newdat group by cluster_id having sum(hashtags_count) > 40 order by 3 desc;

select * from hashtags_prayforparis_15_h limit 10;
