import os
import shelve
import time

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

#adding extra libs
from collections import defaultdict
from urllib.parse import urlparse, urldefrag

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = []

        ## adding more needed attributes
        self.Lock = RLock()
        self.domain_last_access = {}
        self.subdomains = defaultdict(set)
        self.unique_urls = set()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)




    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)

                #building the unique urls set
                self.unique_urls.add(url)

                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):

        # make sure only one thread at a time for the thread-safe purpose
        with self.Lock:
            now = time.time()
            for i, url in enumerate(self.to_be_downloaded):
                domain = urlparse(url).netloc
                last_access = self.domain_last_access.get(domain, 0)
                if now - last_access >= self.config.time_delay:
                    self.domain_last_access[domain] = now
                    return self.to_be_downloaded.pop(i)
            return None

    def add_url(self, url):

        #make sure only one thread at a time for the thread-safe purpose
        with self.Lock:
            url = normalize(url)
            #get rid of the fragment and starting here using fragment_clean version urls
            unfrag_url, _ = urldefrag(url)

            urlhash = get_urlhash(unfrag_url)
            if urlhash not in self.save:
                self.save[urlhash] = (unfrag_url, False)
                self.save.sync()

                self.to_be_downloaded.append(unfrag_url)

                #building up the unique URLs set
                self.unique_urls.add(unfrag_url)
                #checking if the url is missing and also is allowed to be crawled
                if self.check_subdomain(unfrag_url):
                    self.subdomains[unfrag_url.hostname].add(unfrag_url)

    def mark_url_complete(self, url):

        # make sure only one thread at a time for the thread-safe purpose
        with self.Lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()



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




    # 1.How many unique pages did you find?
    #Uniqueness for the purposes of this assignment is ONLY established by the URL,
    # but discarding the fragment part.
    def print_unique_urls(self):
        print(f"There are total of {len(self.unique_urls)} unique pages")



    # 4. How many subdomains did you find in the uci.edu domain?
    def print_subdomains(self):
        print("The following are the subdomains:")
        for subdomain in sorted(self.subdomains):
            print(f"{subdomain}", len(self.subdomains[subdomain]))


