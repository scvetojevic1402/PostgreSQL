--this script was used to check if streaming api includes places that fall completely within given bbox or even only intersect it 
--myUFserver (not the real hostname :) had an old version of postgresql server (9.1 I think --no support for json or jsonb data type) so tweets were stored as base64 encoded varchars
--therefore I used dblink to fetch tweets, decode and process them on a server that has jsonb support (9.5)

create extension if not exists dblink;
create extension if not exists postgis;
select (replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{created_at}')::date,replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,name}',count(id)
 FROM dblink('dbname=twitter_geo_split5 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_nw_ii where insert_time > ''2015-04-26 00:00:00.000000'' and insert_time < ''2015-05-26 00:00:00.000000'' limit 1000)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is not null
            group by (replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{created_at}')::date,replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,name}'
            order by 3 desc;
            --1.179.457

select st_geomfromgejson(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,bounding_box}'),count(id)
 FROM dblink('dbname=twitter_geo_split5 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_nw_ii where insert_time > ''2015-04-27 00:00:00.000000'' and insert_time < ''2015-05-26 00:00:00.000000'' limit 10000)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is null
            group by st_geomfromgejson(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,bounding_box}')
            order by 1 asc;

--EXACT COORDINATES
select round((st_x(ST_GeomFromGeoJSON(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{coordinates}')))::numeric,1),count(id) --ST_GeomFromGeoJSON
 FROM dblink('dbname=twitter_geo_split5 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_sw where insert_time > ''2015-04-26 00:00:00.000000'' and insert_time < ''2015-05-26 00:00:00.000000'' limit 10000)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is not null
            group by round((st_x(ST_GeomFromGeoJSON(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{coordinates}')))::numeric,1)
            order by 1 asc;

--POLYGON CENTROIDS
select round((st_x(st_centroid(ST_MAKEVALID(ST_GeomFromGeoJSON(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,bounding_box}')))))::numeric,1),count(id) --ST_GeomFromGeoJSON
 FROM dblink('dbname=twitter_geo_split5 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
	    ,'(select id, decode(json,''base64'') from tweets_stream_sw where insert_time > ''2015-04-26 00:00:00.000000'' and insert_time < ''2015-05-26 00:00:00.000000'' limit 10000)')
            AS s(id integer,decode bytea) 
            where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is null
            group by round((st_x(st_centroid(ST_MAKEVALID(ST_GeomFromGeoJSON(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,bounding_box}')))))::numeric,1)
            order by 1 asc;
            
select st_crosses(st_makevalid(ST_GeomFromGeoJSON(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,bounding_box}')),ST_MakeLine(ST_MakePoint(-44.5,-85), ST_MakePoint(-44.5,85))),count(id) --ST_GeomFromGeoJSON
		 FROM dblink('dbname=twitter_geo_split5 port=5432 host=myUFgeoserver user=scvetojevic password=myPass'
			    ,'(select id, decode(json,''base64'') from tweets_stream_sw where insert_time > ''2015-04-28 00:00:00.000000'' and insert_time < ''2015-05-26 00:00:00.000000'' limit 10000)')
			    AS s(id integer,decode bytea) 
			    where (replace(convert_from(decode,'UTF8'),'\u0000','')::json->'coordinates'->>'coordinates') is null
			    group by st_crosses(st_makevalid(ST_GeomFromGeoJSON(replace(convert_from(decode,'UTF8'),'\u0000','')::json#>>'{place,bounding_box}')),ST_MakeLine(ST_MakePoint(-44.5,-85), ST_MakePoint(-44.5,85)))
			    order by 1 asc;

SELECT ST_AsText(ST_MakeLine(ST_MakePoint(-180,-0.5), ST_MakePoint(-1,-0.5)));

SELECT relname, relfilenode FROM pg_class WHERE relname = 'urls';
SELECT distinct relnamespace,reltablespace FROM pg_class order by 1 asc;
SELECT * FROM pg_class limit 10;
select oid, datname from pg_database;

select cl.relfilenode, nsp.nspname as schema_name, cl.relname, cl.relkind from pg_class cl join pg_namespace nsp on cl.relnamespace = nsp.oid where nsp.nspname='public';
select pg_relation_filepath('points_4326');
select * from pg_information_schema limit 1;
