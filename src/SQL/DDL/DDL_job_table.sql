CREATE TABLE IF NOT EXISTS contact (
    entry_dt TIMESTAMP WITH TIME ZONE DEFAULT now(),
    lead_id uuid REFERENCES leads(lead_id),
    status_id_phone smallint,
    phone_correction VARCHAR(255),
    status_id_address smallint,
    address_correction VARCHAR(255),
    status_id_name smallint,
    name_correction VARCHAR(255)
    );

|-- search_id: string (nullable = true)
 |-- info_type: string (nullable = true)
 |-- page_number: integer (nullable = true)
 |-- split_n: integer (nullable = true)
 |-- company: string (nullable = true)
 |-- description: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- employment_type: string (nullable = true)
 |-- industries: string (nullable = true)
 |-- job_function: string (nullable = true)
 |-- seniority_level: string (nullable = true)
 |-- job_dt: timestamp (nullable = true)
 |-- job_id: string (nullable = true)
 |-- link: string (nullable = true)
 |-- location: string (nullable = true)
 |-- scrap_time: double (nullable = true)
 |-- title: string (nullable = true)
