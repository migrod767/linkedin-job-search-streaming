CREATE TABLE IF NOT EXISTS job_search (
    search_dt TIMESTAMP ,
    search_id uuid,
    url VARCHAR(500),
    date_posted VARCHAR(50),
    on_site_remote smallint,
    geo_id int,
    keywords VARCHAR(500),
    location VARCHAR(255),
    page_num smallint,
    file_name VARCHAR(255)
    );
