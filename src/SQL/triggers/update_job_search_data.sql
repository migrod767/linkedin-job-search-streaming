--------------------------------------------------------------------------------------------
-- job_searches tablE DDL
-- By      : Miguel Rodriguez
-- Date    : 21-05-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         21-May-2023 - Establishing history
--                   27-May-2023 - Update to working code
--------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION scrapper.update_job_search_data()
RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	with created_list as (
		select js.search_id
		from scrapper.job_searches js
		inner join scrapper.job_searches_spark jss on js.search_id=jss.search_id::uuid
	)

	INSERT INTO scrapper.job_searches (search_dt, search_id, search_url, keywords, search_location, geo_id, on_site_remote)
	SELECT
		search_dt::TIMESTAMP,
	    search_id::uuid,
	    search_url::VARCHAR,
	    keywords::VARCHAR,
	    "location"::VARCHAR,
	    geo_id::INT,
	    on_site_remote::INT
	FROM scrapper.job_searches_spark jss
	where jss.search_id::uuid not in (select * from created_list)
	;

	with created_list as (
		select js.search_id
		from scrapper.job_searches js
		inner join scrapper.job_searches_spark jss on js.search_id=jss.search_id::uuid
	)

	DELETE FROM scrapper.job_searches_spark
	WHERE search_id::uuid in (select * from created_list)
	;

	RETURN NEW;
END;
$$

create OR REPLACE TRIGGER tr_update_job_search_data
AFTER INSERT ON scrapper.job_searches_spark
FOR EACH STATEMENT
EXECUTE function update_job_search_data()
;