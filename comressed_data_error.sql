--assuming we have a tweets(id serial, json jsonb) kind of table that has bad records that can not be selected, this script will 
--print out the ids of the corrupt records. Deletion option can also be added to exception handling block.
do $$
declare
	a int = (select id from tweets  order by 1 asc limit 1);
	b int = (select id from tweets order by 1 desc limit 1);
	t varchar = null;
begin
	raise notice 'a: %; b: %; records to process: %...', to_char(a,'999G999G999G990D00'),to_char(b,'999G999G999G990D00'), to_char(b-a,'999G999G999G990D00');
	while a < b loop
		begin
			select into t select into t substring(json from 1 for 1) from tweets where id = a;
		exception when others then 
			raise notice '% % %', a, SQLERRM, SQLSTATE;
			--delete from tweets where id = a;
		end;
		a = a+1;
		t = null;
		if a%500000 = 0 then
			raise notice '%', to_char(a,'999G999G999G990D00');
		end if;
	end loop;
end$$
