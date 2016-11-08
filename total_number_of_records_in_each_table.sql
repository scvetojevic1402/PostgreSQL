create or replace function total_records_per_db() returns table (schemaname name,relname name, n TEXT)
as $$
begin
	return query SELECT a.schemaname,a.relname,to_char(a.n_live_tup,'999G999G999G990D00') as n FROM pg_stat_user_tables a ORDER BY n_live_tup DESC;
end;
$$language plpgsql;

--usage:
--select (total_records_per_db()).schemaname,(total_records_per_db()).relname,(total_records_per_db()).n;
