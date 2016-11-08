create or replace function total_records() returns text as $$
declare output text :=0;
begin
	select to_char(sum(n_live_tup),'999G999G999G990D00') into output FROM pg_stat_user_tables;
	return output;
end;
$$language plpgsql;

--usage:
--select total_records();
