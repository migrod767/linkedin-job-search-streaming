import datetime
import re
import pandas as pd
import psycopg2
import streamlit as st

st.title('LinkedIn Job Data Search')


# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


conn = init_connection()

query_industries = """
    select industries 
    from scrapper.jobs j 
    group by 1
    order by 1;
    """

query_companies = """
    select company
    from scrapper.jobs j 
    group by 1
    order by 1;
    """

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


# Create a text element and let the reader know the data is loading.
data_load_state = st.text('Loading data...')
industries_list = run_query(query_industries)
industries_list = pd.DataFrame(industries_list)

companies_list = run_query(query_companies)
companies_list = pd.DataFrame(companies_list, columns=['company'])
add_df = pd.DataFrame([''], columns=['company'])
companies_list = pd.concat([companies_list, add_df]).sort_values(by=['company']).reset_index(drop=True)

data_load_state.text("Done!")

option_job_title = st.sidebar.text_input('Job Title Keywords')
option_industry = st.sidebar.selectbox('Industry', industries_list)
option_company = st.sidebar.selectbox('Company', companies_list)



query_search_jobs = """
    select  
    scrape_dt::date as scrape_date,
    title,
    company,
    job_location,
    industries,
    job_id
    from scrapper.jobs j 
    where 1=1
    """
# Create a text element and let the reader know the data is loading.
data_load_state_2 = st.text('Loading data...')

if option_job_title != '':
    option_job_title_str = re.sub(r'[^A-Za-z0-9 ]+', '', option_job_title)
    st.write(option_job_title_str)
    add_where_condition = """and tittle_vector @@ to_tsquery('{0}')""".format(option_job_title_str)
    query_search_jobs = query_search_jobs + add_where_condition

if option_industry != '':
    add_where_condition = """and industries ilike '{0}' """.format(option_industry)
    query_search_jobs = query_search_jobs + add_where_condition

if option_company != '':
    add_where_condition = """and company ilike '{0}' """.format(option_company)
    query_search_jobs = query_search_jobs + add_where_condition

query_search_jobs = query_search_jobs + ';'

#st.write(query_search_jobs)

search_jobs = run_query(query_search_jobs)
search_jobs = pd.DataFrame(search_jobs,
                           columns=['Scrape Date', 'Job Title', 'Company', 'job Location', 'Industry', 'Job Id'])

st.write(f"Results: {search_jobs.shape[0]}")
st.write(search_jobs)

data_load_state_2.text("Done!")



# ------------------------------------------------------------------------------------
st.subheader('Job Details')

query_search_job_id = """
    select  
    scrape_dt::date as scrape_date,
    title,
    company,
    job_location,
    industries,
    seniority_level,
    --description_raw,
    job_url, 
    job_id
    from scrapper.jobs j 
    where 1=1
    """

option_job_id = st.text_input('Job Id')

if option_job_id != '':
    #st.write(f"You selected: {option_job_id}")
    add_where_condition = """and job_id = '{0}';""".format(option_job_id)
    query_search_job_id = query_search_job_id + add_where_condition

    #st.write(query_search_job_id)
    search_jobs = run_query(query_search_job_id)
    search_jobs = pd.DataFrame(search_jobs,
                               columns=['Scrape Date', 'Job Title', 'Company', 'job Location',
                                        'Industry', 'Seniority Level', 'Job URL', 'Job Id'])

    search_jobs = search_jobs.transpose()
    st.write(search_jobs)



