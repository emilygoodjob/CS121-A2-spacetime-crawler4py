from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    # print("[Launch] Starting crawler...")
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    # print("[Launch] Getting cache server...") 
    config.cache_server = get_cache_server(config, restart)
    # print("[Launch] Initializing crawler...")
    crawler = Crawler(config, restart)
    # print("[Launch] Starting crawler execution.")
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    # print(f"[Launch] Config file: {args.config_file}")
    # print(f"[Launch] Restart: {args.restart}")
    main(args.config_file, args.restart)
