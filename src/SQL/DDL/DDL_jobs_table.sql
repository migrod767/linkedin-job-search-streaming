--------------------------------------------------------------------------------------------
-- job table DDL
-- By      : Miguel Rodriguez
-- Date    : 24-04-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         24-Apr-2023 - Establishing history
--------------------------------------------------------------------------------------------

CREATE TABLE scrapper.jobs (
	search_id uuid NULL,
	page_number int4 NULL,
	job_id uuid NULL,
	spark_epoch_id int4 NULL,
	scrape_dt timestamptz NULL,
	job_url varchar(1000) NULL,
	job_posted_dt timestamptz NULL,
	title varchar(255) NULL,
	company varchar(255) NULL,
	job_location varchar(255) NULL,
	scrape_time float NULL,
	description_raw text NULL,
	seniority_level varchar(255) NULL,
	employment_type varchar(255) NULL,
	job_function varchar(500) NULL,
	industries varchar(500) NULL
);

