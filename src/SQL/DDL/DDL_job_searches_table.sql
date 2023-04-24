--------------------------------------------------------------------------------------------
-- job_searches tablE DDL
-- By      : Miguel Rodriguez
-- Date    : 24-04-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         24-Apr-2023 - Establishing history
--------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS job_searches (
    search_dt timestamp with time zone UTC ,
    search_id uuid,
    search_url VARCHAR(1000),
    keywords VARCHAR(500),
    location VARCHAR(255),
    geo_id INT,
    on_site_remote smallint,
    posted_date_filter VARCHAR(255)
    );
