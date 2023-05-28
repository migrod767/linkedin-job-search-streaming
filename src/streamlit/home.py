# streamlit run home.py

import datetime

import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(
        page_title="🏠 Home"
)


st.title('LinkedIn Scraped Data')

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


conn = init_connection()

query_scrape_metrics = """
        select DATE_TRUNC('day', scrape_dt)::DATE as scrape_dt , 
          count(job_id) as counts, 
          avg(j.scrape_time) as avg_scrap_time 
        from scrapper.jobs j 
        group by 1
        ; 
"""

query_scrape_locations = """
        select DATE_TRUNC('day', scrape_dt)::DATE as scrape_dt , job_location , count(job_id) as counts 
        from scrapper.jobs j 
        group by 1, 2
        order by 2 desc
        ;
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
scrape_metrics = run_query(query_scrape_metrics)
scrape_metrics = pd.DataFrame(scrape_metrics, columns=['scrape_dt', 'counts', 'avg_scrape_time'])
scrape_metrics['scrape_dt'] = pd.to_datetime(scrape_metrics['scrape_dt'])
scrape_metrics = scrape_metrics.set_index(pd.DatetimeIndex(scrape_metrics['scrape_dt']))

scrape_locations = run_query(query_scrape_locations)
scrape_locations = pd.DataFrame(scrape_locations, columns=['scrape_dt', 'job_location', 'counts']) \
    .sort_values(by='counts', axis=0, ascending=False)
# scrape_locations['scrape_dt'] = pd.to_datetime(scrape_locations['scrape_dt'])

data_load_state.text("Done!")

if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.subheader('Metrics')
    st.write(scrape_metrics)
    st.subheader('Locations')
    st.write(scrape_locations)

# ------------------------------------------------------------------------------------
st.subheader('Scrape counts by day')
st.bar_chart(scrape_metrics[["counts"]])

# ------------------------------------------------------------------------------------
st.subheader('Avg Scrape Time')
st.bar_chart(scrape_metrics[['avg_scrape_time']])

# ------------------------------------------------------------------------------------
day_to_filter = st.sidebar.date_input('start date', datetime.date(2023, 5, 22))
filtered_data = scrape_locations[scrape_locations['scrape_dt'] == day_to_filter]
st.subheader(f'Job locations by scrap date {day_to_filter}')
st.write(filtered_data)
