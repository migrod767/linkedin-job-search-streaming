--------------------------------------------------------------------------------------------
-- job table DDL
-- By      : Miguel Rodriguez
-- Date    : 24-04-2023
-- Project :
-- Location: src/SQL/DDL
-- History :         24-Apr-2023 - Establishing history
--------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS jobs (
    search_id UUID,
    page_number INT,
    job_id UUID,
    scrape_dt timestamp with time zone UTC ,
    job_url VARCHAR(1000),
    job_posted_dt timestamp with time zone UTC,
    title VARCHAR(255),
    company VARCHAR(255),
    location VARCHAR(255),
    scrape_time TIME,
    description TEXT,
    seniority_leve VARCHAR(255),
    employment_type VARCHAR(255),
    job_function VARCHAR(500),
    industries VARCHAR(500),
    );

