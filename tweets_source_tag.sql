--in this case original table "tweets_stream_ne_i" contained tweets encoded to base64 and was split to ntiles (for some reason :))
do $$
declare
	t varchar = null;
  a int = 1;
  p varchar[] =null;
  m record;
  l record;
  ids bigint[] = null;
begin
	while a < 10 loop
		begin
			raise notice '%', a;
			for l in select id FROM dblink('dbname=YourDB port=5432 host=YourHost user=YourUser password=YourPass'
										  ,'select id from tweets_stream_ne_i where ntile = '||a||')AS s(id integer) 			
			loop
				ids = array_append(ids, l.id::bigint);	
			end loop;
			for m in select id, convert_from(decode::bytea, 'UTF8')::json->>'id' as tweet_id
                     ,substring(convert_from(decode::bytea, 'UTF8')::json->>'source' 
                      from position('>' in convert_from(decode::bytea, 'UTF8')::json->>'source')+1 
                      for position('</' in convert_from(decode::bytea, 'UTF8')::json->>'source')
                                    -position('>' in convert_from(decode::bytea, 'UTF8')::json->>'source')-1) as src
			FROM dblink('dbname=YourDB port=5432 host=YourHost user=YourUser password=YourPass'
			,'select id, decode(json,''base64'') 
        from tweets_stream_ne_i 
        where id=any(''{' || array_to_string(ids, ',') || '}'')')AS s(id bigint, decode bytea)
			LOOP
				raise notice ' % % %',m.id, m.tweet_id, m.src;
				--INSERT INTO lda_src_ne_i(id, tweet_id,src) values (m.id, m.tweet_id::bigint, m.src);
			END LOOP;
		exception when others then 
			raise notice '% % %', a, SQLERRM, SQLSTATE;
		end;
		t = null;
		ids = null;
	a=a+1;
		
	end loop;
end$$
