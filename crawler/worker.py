from threading import Thread

from inspect import getsource
from urllib.parse import urlparse
from utils.download import download
from utils import get_logger
import scraper
import time
import threading
from scraper import max_words_page, global_word_counter


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.worker_id = worker_id
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        # self.logger.info("Worker started") 
        retry_delay = 5  # todo: make this configurable
        max_file_size = 10 * 1024 * 1024  # 10 MB, todo: make this configurable
        min_file_size = 100  # todo: make this configurable
        pages_crawled = 0
        while True:
            tbd_url, next_access_time = self.frontier.get_tbd_url()

            # sleep and retry for 3 times 
            if not tbd_url:
                for _ in range(3):
                    if tbd_url:
                        break
                    if not tbd_url:
                        if next_access_time:
                            self.logger.info(f"Need to wait for the next access time.")
                            if next_access_time > time.time():
                                time.sleep(next_access_time - time.time())
                        else:
                            self.logger.info("No URLs to download. Sleeping for some time.")
                            time.sleep(retry_delay)
                    tbd_url, next_access_time = self.frontier.get_tbd_url()
                    
            if not tbd_url and not next_access_time: # If wait for 3 times and still no url, exit
                self.logger.info("No URLs to download. Exiting.")
                break # while loop will exit
            
            while not tbd_url:
                self.logger.info("Need to wait for the next access time.")
                if next_access_time > time.time():
                    time.sleep(next_access_time - time.time())
                tbd_url, next_access_time = self.frontier.get_tbd_url()
                if not tbd_url and not next_access_time:
                    self.logger.info("No URLs to download. Exiting.")
                    break
                        
            if not scraper.is_valid(tbd_url):
                self.logger.info(f"Skipping invalid URL {tbd_url}. Marking as complete.")
                self.frontier.mark_url_complete(tbd_url)
                self.frontier.sync()
                continue

            # if not scraper.to_crawl(tbd_url):
            #     self.logger.info(f"Skipping trap or low information URL {tbd_url}. Marking as complete.")
            #     self.frontier.mark_url_complete(tbd_url)
            #     self.frontier.sync()
            #     continue

            # download the URL    
            start = time.perf_counter()
            resp = download(tbd_url, self.config, self.logger)
            latency = time.perf_counter() - start
            self.logger.info(f"Latency {latency:.3f}s | {tbd_url} | status {resp.status}")
            
            # Check if the response is valid
            if resp is None:
                self.logger.warning(f"Failed to download {tbd_url}. Skipping.")
                self.frontier.mark_url_complete(tbd_url)
                self.frontier.sync()
                continue
            # Check if response.raw_response is valid
            if resp.raw_response is None:
                self.logger.warning(f"Raw response is None for {tbd_url}. Skipping.")
                self.frontier.sync()
                self.frontier.mark_url_complete(tbd_url)
                continue
            # Skip 404
            if not (200 <= resp.status < 300):
                self.logger.warning(f"Skipping {tbd_url} due to HTTP status {resp.status}.")
                self.frontier.mark_url_complete(tbd_url)
                self.frontier.sync()
                continue
            if resp.status == None:
                self.logger.warning(f"Skipping {tbd_url} due to timeout.")
                self.frontier.mark_url_complete(tbd_url)
                self.frontier.sync()
                continue
            # Check if the content length is too large
            content_length = resp.raw_response.headers.get("Content-Length")
            if content_length and int(content_length) > max_file_size:
                self.logger.info(f"Skipping {tbd_url} due to large file size ({content_length} bytes).")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # Check if the response content is too small
            if len(resp.raw_response.content) < min_file_size:
                self.logger.info(f"Skipping {tbd_url} because content is too small ({len(resp.raw_response.content)} bytes).")
                self.frontier.mark_url_complete(tbd_url)
                self.frontier.sync()
                continue

            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            pages_crawled += 1
            if pages_crawled % 10 == 0:
                self.logger.info("Auto-saving frontier state...")
                self.frontier.sync()

            # sleep for the crawl delay before the next request
            time.sleep(self.config.time_delay)

