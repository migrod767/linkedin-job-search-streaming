--------------------------------------------------------------------------------------------
-- job_searches tablE DDL
-- By      : Miguel Rodriguez
-- Date    : 24-04-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         24-Apr-2023 - Establishing history
--------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scrapper.job_searches (
    search_dt TIMESTAMP WITH TIME ZONE,
    search_id uuid PRIMARY KEY,
    search_url VARCHAR(1000),
    keywords VARCHAR(500),
    search_location VARCHAR(255),
    geo_id INT,
    on_site_remote smallint,
    posted_date_filter VARCHAR(255)
    );
