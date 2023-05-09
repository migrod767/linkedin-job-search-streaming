# source: https://www.scraperapi.com/blog/linkedin-scraper-python/

import json
import os
import time
import uuid
from datetime import datetime
from datetime import timezone
from urllib.parse import parse_qs
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


class LinkedinScrapper:
    def __init__(self):
        self.jobs_per_json = 10
        self.n_scrap_pages = 3
        self.sleep_between_requests = 0.5

        self.scraperapi_on_off = False  # 1=on 0=off
        self.scraperapi_key = '0d838c8cdb4a90405e5ab4a4997a64a1'

        self.json_file_on_off = True
        self.output_dir = os.path.join('../..', 'data')
        self.search_url = None
        self.search_id = None

        self.json_name_dt_format = "%Y-%m-%d_%H-%M-%S"
        self.current_date_time = None
        self.current_date_time_file_fmt = None

        self.ingestion_api_on_off = True
        self.ingestion_api_url = 'http://127.0.0.1:8000'
        self.ingestion_api_job_endpoint = '/ingest/job'
        self.ingestion_api_search_endpoint = '/ingest/search'

    def send_outbound_information(self, outbound_info, json_file_name, data_type):
        if self.json_file_on_off is True:
            # Drops the search information into a json file
            with open(os.path.join(self.output_dir, json_file_name), 'w') as fp:
                json.dump(outbound_info, fp)
            print(f"Writing json file : {json_file_name}")

        # Push the information to ingest api
        if self.ingestion_api_on_off is True:
            if data_type == 'job':
                ingest_url = self.ingestion_api_url + self.ingestion_api_job_endpoint
            elif data_type == 'search':
                ingest_url = self.ingestion_api_url + self.ingestion_api_search_endpoint

            try:
                outbound_json = {"Message": json.dumps(outbound_info)}
                response = requests.post(ingest_url, json=outbound_json)
                print("ingest_api response: " + str(response.status_code))
            except Exception as e:
                raise Exception(e)

    def search_scraper(self, page_number):
        """
        Gets the information of the search, starts looping over the jobs and the pages of the url.
        :param page_number:
        :return:
        """
        # Scraps the url using the pagination number (page_number)
        print(f"Scraping page number {page_number}")
        next_page = self.search_url + str(page_number)

        search_info = {'search_id': self.search_id,
                       'page_number': page_number}

        # Change the url request using Scraper Api
        scraperapi_url = self.get_request_url(next_page)

        # Make the request to LinkedIn
        response = requests.get(str(scraperapi_url))
        # Parse the result using B4S
        soup = BeautifulSoup(response.content, 'html.parser')
        # Get a list of the jobs found in the response
        jobs = soup.find_all('div',
                             class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')

        # For each job post pull the important information
        for idx, job in enumerate(jobs, start=1):
            start = time.time()
            now = datetime.now(timezone.utc)  # current date and time
            scrape_dt = now.isoformat()
            job_title = job.find('h3', class_='base-search-card__title').text.strip()
            print(f"getting job {job_title}...")
            job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
            job_location = job.find('span', class_='job-search-card__location').text.strip()
            job_link = job.find('a', class_='base-card__full-link')['href']
            # The job_scraper goes to the job url and pulls the description information.
            job_description = self.job_url_scraper(job_link)
            job_id = str(uuid.uuid4())

            end = time.time()
            scrap_time = "{time:6.2f}".format(time=(end - start))

            job_dict = {
                'job_id': job_id,
                'scrape_dt': scrape_dt,
                'title': job_title,
                'company': job_company,
                'location': job_location,
                'link': job_link,
                'scrape_time': scrap_time
            }
            job_dict.update(job_description)

            outbound_data = search_info | job_dict

            json_file_name = f'job_{now.strftime(self.json_name_dt_format)}.json'
            self.send_outbound_information(outbound_data, json_file_name, 'job')

            end = time.time()
            print(f"scrape time: {end - start:6.2f} s \n")

    def job_url_scraper(self, job_webpage):
        """
        This method calls the job post url and pulls the description and other information from the page.
        :param job_webpage: is the url of the job post
        :return:
        """
        scraperapi_url = self.get_request_url(job_webpage)
        response = requests.get(str(scraperapi_url))
        soup = BeautifulSoup(response.content, 'html.parser')
        # Get when the job post was posted
        try:
            date_posted = json.loads(soup.find('script', type='application/ld+json').text)['datePosted']
            date_posted = datetime.strptime(date_posted, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).isoformat()
        except:
            # TODO Log not able to get posted date
            date_posted = ""

        # Get description from the job post
        description_bs4 = soup.find_all('div', class_='show-more-less-html__markup')
        stringfy_desc = []

        for items in description_bs4:
            for row in items.find_all():
                stringfy_desc.append(str(row))

        json_desc = {'description': stringfy_desc}

        description_criteria_bs4 = soup.find_all('ul', class_='description__job-criteria-list')
        criteria_dict = {}

        for item in description_criteria_bs4:
            for item2 in item.find_all():
                if item2.name == 'li':
                    key = item2.find_all()[0].text.replace('\n', ' ').strip().lower().replace(' ', '_')
                    value = item2.find_all()[1].text.replace('\n', ' ').strip()
                    criteria_dict[key] = value

        json_desc['description_criteria'] = criteria_dict
        json_desc['job_date_posted'] = date_posted

        return json_desc

    def get_url_params(self):
        """
        Pulls the url query params for the search url and returns them as a dict
        :return:
        """
        if self.search_url is None:
            raise Exception("There is no search url please load one.")

        parsed_url = urlparse(self.search_url)
        url_params = parse_qs(parsed_url.query)

        clean_dict = {}
        for key in url_params.keys():
            clean_dict[key] = url_params[key][0]

        return clean_dict

    def get_request_url(self, target_url):
        """
        Changes the url using Scraper API if it is active
        :param target_url:
        :return:
        """

        if self.scraperapi_on_off is True:
            scraperapi_url = f'http://api.scraperapi.com?api_key={self.scraperapi_key}&url={target_url}'

        elif self.scraperapi_on_off is False:
            scraperapi_url = target_url

        else:
            raise Exception("scraperapi_on_off not defined")

        time.sleep(self.sleep_between_requests)
        return scraperapi_url

    def run_scrapper(self, webpage):
        """
        This is the main method, it gets the LinkedIn job search url and starts looping through the pages.
        :param webpage:
        :return:
        """
        print("Start scrapper...")
        total_start = time.time()
        self.search_url = webpage
        params = self.get_url_params()

        self.search_id = str(uuid.uuid4())
        self.current_date_time = datetime.now(timezone.utc)
        file_dt = self.current_date_time.strftime(self.json_name_dt_format)
        print(f"timestamp: {file_dt}")

        search_data = {
            'search_id': self.search_id,
            'url': self.search_url,
            'search_dt': self.current_date_time.isoformat(),
            'url_params': params
        }
        print(f"url params: {params}")

        json_file_name = f'search_{file_dt}.json'
        self.send_outbound_information(search_data, json_file_name, 'search')

        # Loops through the pages of the search
        for page_number in range(0, 25 * self.n_scrap_pages, 25):
            self.search_scraper(page_number)

        total_end = time.time()
        print(f"Scrapper time: {total_end - total_start:6.2f} s \n")


if __name__ == '__main__':
    keyword = 'data%20engineer'
    location = 'Florida%2C%20United%20States'
    geo_id = 101318387
    f_TPR = 'r604800'  # Search posting from the last week
    f_WT = 3  # 1=On-site 2=remote 3=Hybrid

    search_link = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?' \
                  f'keywords={keyword}&location={location}&geoId={geo_id}' \
                  f'&f_TPR={f_TPR}&f_WT={f_WT}&trk=public_jobs_jobs-search-bar_search-submit' \
                  f'&position=1&pageNum=0&start='

    linkedin_scrapper = LinkedinScrapper()
    linkedin_scrapper.run_scrapper(search_link)
