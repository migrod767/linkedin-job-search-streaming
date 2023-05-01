--------------------------------------------------------------------------------------------
-- job table DDL
-- By      : Miguel Rodriguez
-- Date    : 24-04-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         24-Apr-2023 - Establishing history
--------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scrapper.jobs (
    search_id UUID,
    page_number INT,
    job_id UUID,
    spark_epoch_id INT,
    scrape_dt TIMESTAMP WITH TIME ZONE,
    job_url VARCHAR(1000),
    job_posted_dt TIMESTAMP WITH TIME ZONE,
    title VARCHAR(255),
    company VARCHAR(255),
    job_location VARCHAR(255),
    scrape_time TIME,
    description_raw TEXT,
    seniority_leve VARCHAR(255),
    employment_type VARCHAR(255),
    job_function VARCHAR(500),
    industries VARCHAR(500)
    );

