from threading import Thread

from inspect import getsource
from urllib.parse import urlparse
from utils.download import download
from utils import get_logger
import scraper
import time
import threading
from robotexclusionrulesparser import RobotFileParser


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):

        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.robot_parsers = {} # store robots.txt parsers for each domain
        
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)


    def get_crawl_delay(self, domain):
        """
        Get the crawl delay for the domain from robots.txt.
        """
        rp = self.robot_parsers.get(domain)
        if rp is None:
            return self.config.time_delay
        
        # Get Crawl-Delay
        crawl_delay = rp.get_crawl_delay("*")
        if crawl_delay is None:
            return self.config.time_delay
        return crawl_delay


    def can_fetch(self, url):
        """
        Check if the URL can be fetched based on robots.txt rules.
        """
        parsed_url = urlparse(url)
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        if domain not in self.robot_parsers:
            # Download and parse robots.txt
            robots_url = f"{domain}/robots.txt"
            self.logger.info(f"Fetching robots.txt from {robots_url}")
            rp = RobotFileParser()
            try:
                rp.fetch(robots_url)
                self.robot_parsers[domain] = rp
            except Exception as e:
                self.logger.warning(f"Failed to fetch robots.txt from {robots_url}: {e}")
                self.robot_parsers[domain] = None
        
        rp = self.robot_parsers.get(domain)
        if rp is None:
            return True
        
        # To check if the URL can be fetched
        return rp.is_allowed("*", url)

        
    def run(self):
        retry_delay = 5  # todo: make this configurable
        max_file_size = 10 * 1024 * 1024  # 10 MB, todo: make this configurable
        min_file_size = 100  # todo: make this configurable
        while True:
            tbd_url = self.frontier.get_tbd_url()

            # sleep and retry
            if not tbd_url:
                # sleep for a while before checking again
                self.logger.info(f"Frontier is empty. Sleeping for {retry_delay}s.")
                time.sleep(retry_delay)
                tbd_url = self.frontier.get_tbd_url()
                if not tbd_url:
                    # if still empty, stop the crawler
                    self.frontier.stop()
                    self.frontier.save()
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                else:
                    continue

            # check if the URL can be fetched based on robots.txt rules
            if not self.can_fetch(tbd_url):
                self.logger.info(f"Skipping {tbd_url} due to robots.txt rules.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # get crawl delay for the domain
            parsed_url = urlparse(tbd_url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            crawl_delay = self.get_crawl_delay(domain)

            # download the URL    
            resp = download(tbd_url, self.config, self.logger)

            # Check if the response is valid
            if resp is None:
                self.logger.warning(f"Failed to download {tbd_url}. Skipping.")
                self.frontier.mark_url_complete(tbd_url)
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
                continue

            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)

            # sleep for the crawl delay before the next request
            self.logger.info(f"Sleeping for {crawl_delay}s before next request.")
            time.sleep(crawl_delay)


def start_workers(config, frontier):
    """
    Start a worker thread.
    """
    workers = []

    # start all the worker threads
    for worker_id in range(config.threads_count):
        logger = get_logger(f"Worker-{worker_id}", "Worker")
        worker = Worker(worker_id, config, frontier)
        workers.append(worker)
        worker.start()
        logger.info(f"Worker-{worker_id} started.")

    # print status of all the workers
    def print_status_loop():
        interval = 120 # seconds # todo: make this configurable
        while True:
            alive = sum(1 for w in workers if w.is_alive())
            qsize = frontier.queue_size()
            logger.info(
                f"Status: active_workers={alive}, queue_size={qsize}"
            )
            if alive == 0:
                break
            time.sleep(interval)

    status_thread = threading.Thread(
        target=print_status_loop, name="StatusThread", daemon=True
    )
    status_thread.start()

    # wait for all the workers to finish
    for w in workers:
        w.join()
    status_thread.join()
    logger.info("All workers finished")