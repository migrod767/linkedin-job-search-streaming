--------------------------------------------------------------------------------------------
-- job_searches tablE DDL
-- By      : Miguel Rodriguez
-- Date    : 21-05-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         21-May-2023 - Establishing history
--------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION scrapper.update_job_data()
RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	with created_list as (
		select j.job_id
		from scrapper.jobs j
		inner join scrapper.jobs_spark js on j.job_id=js.job_id::uuid
	)

	INSERT INTO scrapper.jobs (search_id, page_number, spark_epoch_id, job_id,	scrape_dt, job_url,	job_posted_dt, title,
        company, job_location, scrape_time, description_raw, seniority_level, employment_type,
        job_function, industries)
	SELECT
        search_id::UUID,
        page_number::INT,
        spark_epoch_id::INT,
        job_id::UUID,
        scrape_dt::TIMESTAMP,
        job_url::VARCHAR,
        job_posted_dt::TIMESTAMP,
        title::VARCHAR,
        company::VARCHAR,
        job_location::VARCHAR,
        scrape_time::TIME,
        description_raw::TEXT,
        seniority_level::VARCHAR,
        employment_type::VARCHAR,
        job_function::VARCHAR,
        industries::VARCHAR
	FROM scrapper.jobs_spark js
	where js.job_id::uuid not in (select * from created_list)
	;

	with created_list as (
		select js.job_id
		from scrapper.jobs js
		inner join scrapper.jobs_spark jss on js.job_id=jss.job_id::uuid
	)

	DELETE FROM scrapper.jobs_spark
	WHERE job_id::uuid in (select * from created_list);

	RETURN NEW;
END;
$$

create OR REPLACE TRIGGER tr_update_job_data
AFTER INSERT ON scrapper.job_spark
FOR EACH STATEMENT
EXECUTE function update_job_data()
;