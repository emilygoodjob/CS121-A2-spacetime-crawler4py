import os
import shelve
import time

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
import scraper
from scraper import is_valid, EXACT_DUP_FILE, NEAR_DUP_FILE, STATE_FILE

#adding extra libs
from collections import defaultdict
from urllib.parse import urlparse, urldefrag
import heapq
from collections import deque

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = []
        self.domain_queues  = defaultdict(deque)
        self.domain_next    = defaultdict(float)
        self.ready_heap     = []

        ## adding more needed attributes
        self.Lock = RLock()
        self.domain_last_access = {}
        self.subdomains = defaultdict(set)
        self.unique_urls = set()
        self.discovered = 0
        self.completed = 0
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
            if os.path.exists(EXACT_DUP_FILE):
                os.remove(EXACT_DUP_FILE)
            if os.path.exists(NEAR_DUP_FILE):
                os.remove(NEAR_DUP_FILE)
            if os.path.exists(STATE_FILE):
                os.remove(STATE_FILE)
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
            if os.path.exists(EXACT_DUP_FILE):
                os.remove(EXACT_DUP_FILE)
            if os.path.exists(NEAR_DUP_FILE):
                os.remove(NEAR_DUP_FILE)
            if os.path.exists(STATE_FILE):
                os.remove(STATE_FILE)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        scraper.seen_hashes = scraper.load_dup_state(EXACT_DUP_FILE, set())
        scraper.seen_shingles = scraper.load_dup_state(NEAR_DUP_FILE, {})
        scraper.load_state_file(self)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            if len(self.save) == 0:
                for url in self.config.seed_urls:
                    self.add_url(url)
            self._parse_save_file()




    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0

        for url, completed in self.save.values():
            self.discovered += 1
            if completed:
                self.completed += 1
                continue
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                #building the unique urls set
                self.unique_urls.add(url)
                tbd_count += 1
            
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        """
        Get the next URL to be downloaded from the frontier.
        """
          
        # with self.lock:
        #     while self.ready_heap:
        #         # pop the next domain to be downloaded
        #         next_t, domain = heapq.heappop(self.ready_heap)
        #         now = time.time()

        #         if next_t > now:
        #             # If the next time is in the future, push it back and return None
        #             heapq.heappush(self.ready_heap, (next_t, domain))
        #             self.logger.info(f"Waiting for {domain} to be ready.")
        #             return None                      

        #         dq = self.domain_queues[domain]
        #         # if the queue is empty, continue to the next domain
        #         if not dq:       
        #             continue
               
        #         url = dq.popleft()

        #         # set the next time to be downloaded for this domain
        #         # and push it back to the heap
        #         self.domain_next[domain] = now + self.politeness
        #         if dq: 
        #             heapq.heappush(self.ready_heap,
        #                            (self.domain_next[domain], domain))
        #         return url
        #     return None
        
        # make sure only one thread at a time for the thread-safe purpose
        with self.Lock:
            smallest_next_access_time = time.time() + self.config.time_delay
            for i, url in enumerate(self.to_be_downloaded):
                domain = urlparse(url).netloc 
                last_access = self.domain_last_access.get(domain, 0) 
                crawl_delay = self.config.time_delay
                now = time.time()
                smallest_next_access_time = min(smallest_next_access_time, last_access + crawl_delay)
                # Respect the crawl delay
                if now - last_access >= crawl_delay:
                    self.domain_last_access[domain] = now
                    return self.to_be_downloaded.pop(i), None
            if self.to_be_downloaded: # if there are still urls in the queue but not ready to be downloaded yet
                self.logger.info(f"Waiting for {smallest_next_access_time - time.time()} seconds to download the next URL.")
                return None, smallest_next_access_time
            else: # if there are no urls in the queue, return None
                self.logger.info("No URLs to download.")
                return None, None
        

    def add_url(self, url):

        # make sure only one thread at a time for the thread-safe purpose
        with self.Lock:
            url = normalize(url)
            
            # check if the url is valid
            if not is_valid(url):
                return
            
            # get rid of the fragment and starting here using fragment_clean version urls
            unfrag_url, _ = urldefrag(url)

            urlhash = get_urlhash(unfrag_url)
            if urlhash in self.save:
                self.logger.info(f"URL already in the frontier or completed: {url}")
                return
            
            if urlhash not in self.save:
                self.logger.info(f"Adding URL to frontier: {unfrag_url}") 
                self.save[urlhash] = (unfrag_url, False)
                self.save.sync()

                self.to_be_downloaded.append(unfrag_url)

                self.discovered +=1 # discovered means that the url is added to the frontier

                #building up the unique URLs set
                self.unique_urls.add(unfrag_url)
                parsed_unfrag = urlparse(unfrag_url)
                hostname = parsed_unfrag.hostname
                # Check which subdomain the URL is belonging to
                if hostname and self.check_subdomain(unfrag_url):
                    self.subdomains[hostname].add(unfrag_url)
                    

    def mark_url_complete(self, url):

        # make sure only one thread at a time for the thread-safe purpose
        with self.Lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            self.logger.info(f"Marking URL as complete: {url}")
            self.completed += 1
            self.save[urlhash] = (url, True)
            self.save.sync()
            if not self.to_be_downloaded:
                self.logger.info("Frontier is empty.")



    # checking if the url is following the instruction of
    #*.ics.uci.edu/*
    #*.cs.uci.edu/*
    #*.informatics.uci.edu/*
    #*.stat.uci.edu/*
    #today.uci.edu/department/information_computer_sciences/*
    def check_subdomain(self, url):
        try:
            parsed_url = urlparse(url)
            hostname = parsed_url.hostname
            path = parsed_url.path

            if hostname is None:
                return False

            assigned_domains = (
                ".ics.uci.edu",
                ".cs.uci.edu",
                ".informatics.uci.edu",
                ".stat.uci.edu"
            )

            if any(hostname.endswith(domain) for domain in assigned_domains):
                return True

            if hostname == ("today.uci.edu") and path.startswith("/department/information_computer_sciences"):
                return True

            return False

        except Exception as e:
            self.logger.error(f"Error on {url}: {e}")
            return False


    def get_status(self):
        """
        Return a dict of the information of the frontier.
        """
        with self.Lock:
            return {
                "total_discovered": self.discovered,
                "queue_size": len(self.to_be_downloaded),
                "completed": self.completed,
            }



    # 1.How many unique pages did you find?
    # Uniqueness for the purposes of this assignment is ONLY established by the URL,
    # but discarding the fragment part.
    def print_unique_urls(self):
        self.logger.info(f"There are total of {len(self.unique_urls)} unique pages")



    # 4. How many subdomains did you find in the uci.edu domain?
    def print_subdomains(self):
        self.logger.info("The following are the subdomains:")
        for subdomain in sorted(self.subdomains):
            self.logger.info(f"{subdomain}", len(self.subdomains[subdomain]))

    def sync(self):
        with self.Lock:
            self.save.sync()
            
    def queue_size(self):
        with self.Lock:
            return len(self.to_be_downloaded)

