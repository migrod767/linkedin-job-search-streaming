# source: https://www.scraperapi.com/blog/linkedin-scraper-python/

import csv
import requests
from bs4 import BeautifulSoup
import json
import os
from urllib.parse import urlparse
from urllib.parse import parse_qs
import uuid
from datetime import datetime
import time
from kafka_controller import KafkaProducer


class LinkedinScrapper:
    def __init__(self):
        self.jobs_per_json = 10
        self.n_scrap_pages = 10
        self.sleep_between_requests = 1
        self.scraperapi_key = '0d838c8cdb4a90405e5ab4a4997a64a1'
        self.scraperapi_on_off = 0  #1=on 0=off
        self.output_dir = os.path.join('..', 'data')
        self.search_url = ''
        self.search_id = ''
        self.current_date_time = ''

    def search_scraper(self, page_number):
        """

        :param page_number:
        :return:
        """
        # Scraps the url using the pagination number (page_number)
        print(f"Scraping page number {page_number}")
        next_page = self.search_url + str(page_number)

        # Change the url request using Scraper Api
        scraperapi_url = self.scraperapi_request_url(next_page)

        # Make the request to LinkedIn
        response = requests.get(str(scraperapi_url))
        # Parse the result using B4S
        soup = BeautifulSoup(response.content, 'html.parser')
        # Get a list of the jobs found in the response
        jobs = soup.find_all('div',
                             class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')

        jobs_list = []
        split_n = 0
        lst_len = len(jobs)
        # For each job post pull the important information
        for idx, job in enumerate(jobs, start=1):
            start = time.time()
            now = datetime.now()  # current date and time
            current_date_time = now.strftime("%Y-%m-%d_%H-%M-%S")
            job_title = job.find('h3', class_='base-search-card__title').text.strip()
            print(f"getting job {job_title}...")
            job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
            job_location = job.find('span', class_='job-search-card__location').text.strip()
            job_link = job.find('a', class_='base-card__full-link')['href']
            # The job_scraper goes to the job url and pulls the description information.
            job_description = self.job_url_scraper(job_link)
            job_id = str(uuid.uuid4())

            end = time.time()
            scrap_time = "{time:6.2f}".format(time=(end-start))

            job_dict = {
                'job_id': job_id,
                'job_dt': current_date_time,
                'title': job_title,
                'company': job_company,
                'location': job_location,
                'link': job_link,
                'scrap_time': scrap_time
            }
            job_dict.update(job_description)
            jobs_list.append(job_dict)

            end = time.time()
            print(f"scrap time: {end - start:6.2f} s \n")

            if idx % self.jobs_per_json == 0:
                self.write_jobs_json(page_n=page_number, split_n=split_n, jobs_info=jobs_list)
                jobs_list = []
                split_n = split_n + 1

            if (idx == lst_len) and (len(jobs_list) != 0):
                self.write_jobs_json(page_n=page_number, split_n=split_n, jobs_info=jobs_list)

    def write_jobs_json(self, page_n, split_n, jobs_info):
        """
        With the job information scrapped it gets dumped into a json file.
        :param page_n: Is the page number
        :param split_n: The json file only saves N jobs per file,
        :param jobs_info:
        :return:
        """
        file_dict = {'search_id': self.search_id,
                     'info_type': 'job',
                     'page_number': page_n,
                     'split_n': split_n,
                     'jobs': jobs_info
                     }
        json_file_name = f'jobs_{self.current_date_time}_{page_n}_{split_n}.json'
        with open(os.path.join(self.output_dir, json_file_name), 'w') as fp:
            json.dump(file_dict, fp)

        print(f"Dumping job info to {json_file_name}")
        print(f"Jobs {len(jobs_info)} \n")

    def job_url_scraper(self, job_webpage):
        """
        This method calls the job post url and pulls the description and other information from the page.
        :param job_webpage: is the url of the job post
        :return:
        """
        scraperapi_url = self.scraperapi_request_url(job_webpage)
        response = requests.get(str(scraperapi_url))
        soup = BeautifulSoup(response.content, 'html.parser')

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

        return json_desc

    def url_params(self):
        """
        Pulls the url query params for the search url to save the parameters used in the search
        :return:
        """
        parsed_url = urlparse(self.search_url)
        url_params = parse_qs(parsed_url.query)

        clean_dict = {}
        for key in url_params.keys():
            clean_dict[key] = url_params[key][0]

        return clean_dict

    def scraperapi_request_url(self, target_url):
        """
        Changes the url using Scraper API if it is active
        :param target_url:
        :return:
        """

        if self.scraperapi_on_off == 1:
            scraperapi_url = f'http://api.scraperapi.com?api_key={self.scraperapi_key}&url={target_url}'

        elif self.scraperapi_on_off == 0:
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
        params = self.url_params()

        self.search_id = str(uuid.uuid4())
        now = datetime.now()  # current date and time
        self.current_date_time = now.strftime("%Y-%m-%d_%H-%M-%S")
        print(f"timestamp: {self.current_date_time}")

        search_info = {
            'search_id': self.search_id,
            'info_type': 'search',
            'url': self.search_url,
            'search_dt': self.current_date_time,
            'url_params': params
        }
        print(f"url params: {params}")

        # Drops the search information into a json file
        # TODO I think this will change if we are going to use mongo
        json_file_name = f'search_{self.current_date_time}.json'
        with open(os.path.join(self.output_dir, json_file_name), 'w') as fp:
            json.dump(search_info, fp)
        print(f"search file name: {json_file_name}")

        # Loops through the pages of the search
        for page_number in range(0, 25 * self.n_scrap_pages, 25):
            self.search_scraper(page_number)

        total_end = time.time()
        print(f"Scrapper time: {total_end - total_start:6.2f} s \n")


if __name__ == '__main__':
    keyword = 'data%20engineer'
    location = 'Florida%2C%20United%20States'
    geo_id = 101318387
    f_TPR = 'r604800'  #Search posting from the last week
    f_WT = 3  #1=On-site 2=remote 3=Hybrid

    search_link = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?' \
                  f'keywords={keyword}&location={location}&geoId={geo_id}' \
                  f'&f_TPR={f_TPR}&f_WT={f_WT}&trk=public_jobs_jobs-search-bar_search-submit' \
                  f'&position=1&pageNum=0&start='

    linkedin_scrapper = LinkedinScrapper()
    linkedin_scrapper.run_scrapper(search_link)






