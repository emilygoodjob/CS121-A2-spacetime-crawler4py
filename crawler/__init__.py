from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from pathlib import Path
import time
import threading
from scraper import save_dup_state, save_state_file, max_words_page, global_word_counter

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

        # Loop for printing status in logger file and text file    
        def print_status_loop():
            interval = 120 # seconds # todo: make this configurable
            while True:
                alive = sum(1 for w in self.workers if w.is_alive())
                st = self.frontier.get_status()
                self.logger.info(
                    f"Status: active_workers={alive}, queue_size={st['queue_size']}"
                )
                self.logger.info(
                    "Status: discovered=%d  queue=%d  completed=%d",
                    st["total_discovered"], st["queue_size"], st["completed"]
                )
                if alive == 0:
                    break
                time.sleep(interval)
        self.status_thread = threading.Thread(
            target=print_status_loop,
            name="StatusThread",
            daemon=True
        )
        self.status_thread.start()

        # Loop for printing status in text file
        def update_status_file():
            interval = 120
            outfile = Path("crawl_stats.txt")
            while True:
                with self.frontier.Lock:
                    unique_url_count = len(self.frontier.unique_urls)
                    subdomains = {
                        sd: len(urls)
                        for sd, urls in self.frontier.subdomains.items()
                    }
                from scraper import max_words_page, global_word_counter
                longest_url, max_words = max_words_page
                top50: list[tuple[str,int]] = global_word_counter.most_common(50)

                lines = []
                lines.append(f"UNIQUE URLS (count): {unique_url_count}\n")
                lines.append("SUBDOMAINS (alphabetical)\n")
                for sd in sorted(subdomains):
                    lines.append(f"{sd}, {subdomains[sd]}")
                lines.append("")
                lines.append("LONGEST PAGE:")
                if longest_url:
                    lines.append(f"URL : {longest_url}")
                    lines.append(f"Word Count : {max_words}")
                else:
                    lines.append("N/A")
                lines.append("")
                lines.append("TOP-50 WORDS:")
                for w, f in top50:
                    lines.append(f"{w:} {f}")
                lines.append("")

                outfile.write_text("\n".join(lines), encoding="utf-8")
                if not any(w.is_alive() for w in self.workers):
                    break
                time.sleep(interval)
        self.print_thread = threading.Thread(
            target=update_status_file,
            name="PrintThread",
            daemon=True
        )
        self.print_thread.start()
        
        # Loop for save state
        def periodic_save():
            while True:
                time.sleep(120)
                save_state_file(max_words_page, global_word_counter)
                save_dup_state()

        threading.Thread(target=periodic_save, daemon=True).start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
