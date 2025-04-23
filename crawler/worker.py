from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
import threading


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):

        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        time_delay = self.config.time_delay
        retry_delay = 5 # todo: make this configurable
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

            # download the URL    
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(time_delay)


def start_workers(config, frontier):
    """
    Start a worker thread.
    """
    logger = get_logger(f"Worker-{worker_id}", "Worker")
    workers = []

    # start all the worker threads
    for worker_id in range(config.threads_count):
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
