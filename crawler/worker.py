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
            tbd_url = self.frontier.get_tbd_url()

            # sleep and retry
            if not tbd_url:
                # sleep for a while before checking again
                self.logger.info(f"Frontier is empty. Sleeping for {retry_delay}s.")
                time.sleep(retry_delay)
                tbd_url = self.frontier.get_tbd_url()
                if not tbd_url:
                    # if still empty, stop the crawler
                    # self.frontier.stop()
                    self.frontier.sync()
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    
                    # log report
                    if self.worker_id == 0:
                        self.frontier.print_unique_urls()
                        self.frontier.print_subdomains()
                        self.frontier.logger.info(f"\nPage with most words: {max_words_page[0]} ({max_words_page[1]} words)")
                        self.frontier.logger.info("\nTop 50 words:")
                        for word, freq in global_word_counter.most_common(50):
                            self.frontier.logger.info(f"{word}: {freq}")
                        
                    break
                else:
                    continue

            # download the URL    
            resp = download(tbd_url, self.config, self.logger)

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
            st = frontier.get_status()
            logger.info(
                f"Status: active_workers={alive}, queue_size={qsize}"
            )
            frontier.logger.info(
                "Status: discovered=%d  queue=%d  completed=%d",
                st["total_discovered"], st["queue_size"], st["completed"]
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

