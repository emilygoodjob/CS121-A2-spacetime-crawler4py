import re
import hashlib
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from collections import Counter
import threading
import atexit
import pickle
import os

# Duplicate detection
seen_hashes = set()
seen_shingles = dict()
global_word_counter = Counter()
max_words_page = ("", 0)
seen_shingles_lock = threading.Lock()

# Duplicate load and save
EXACT_DUP_FILE = 'seen_hashes.pkl'
NEAR_DUP_FILE = 'seen_shingles.pkl'

def load_dup_state(filepath, default):
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    return default

seen_hashes = load_dup_state(EXACT_DUP_FILE, set())
seen_shingles = load_dup_state(NEAR_DUP_FILE, {})

def save_dup_state():
    with open(EXACT_DUP_FILE, 'wb') as f:
        pickle.dump(seen_hashes, f)
    with open(NEAR_DUP_FILE, 'wb') as f:
        pickle.dump(seen_shingles, f)
    print("[INFO] Saved exact and near duplicate states.")

# Store state file
STATE_FILE = "crawl_stats.pkl"

def save_state_file(max_words_page, global_word_counter):
    state = {
        "max_words_page": max_words_page,
        "global_word_counter": global_word_counter
    }
    with open(STATE_FILE, "wb") as f:
        pickle.dump(state, f)
    print("State saved to", STATE_FILE)

def load_state_file():
    global max_words_page, global_word_counter
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "rb") as f:
            state = pickle.load(f)
        max_words_page = state["max_words_page"]
        global_word_counter.update(state["global_word_counter"])
        print("State loaded from", STATE_FILE)

load_state_file()

current_dir = os.path.dirname(os.path.abspath(__file__))

stopwords_path = os.path.join(current_dir, "stopwords.txt")

with open(stopwords_path, "r", encoding="utf-8") as f:
    STOPWORDS = set(line.strip() for line in f)

SHINGLE_SIZE = 5
NEAR_DUPLICATE_THRESHOLD = 0.6

def scraper(url, resp):
    global global_word_counter
    global max_words_page
    
    # handle successful responses
    if resp.status != 200 or not resp.raw_response:
        print(f"Non-200 response ({resp.status}) for: {url}")
        return []
    text = extract_visible_text(resp)
    words = [w.lower() for w in re.findall(r'\b\w+\b', text)]

    # add counts
    
    filtered_words = [w for w in words if w not in STOPWORDS and not w.isdigit()]
    
    # Detect and avoid dead URLs that return a 200 status but no data
    if len(filtered_words) < 100:
        print(f"Dead or low-information page: {url}")
        return []
    
    global_word_counter.update(filtered_words)
    # check if this page has most words
    if len(filtered_words) > max_words_page[1]:
        max_words_page = (url, len(filtered_words))
    
    # check exact duplicates & near duplicates
    if is_exact_duplicate(text):
        print(f"Exact Duplicate: {url}")
        return []
    is_near, other_url, similarity = is_near_duplicate(url, text)
    if is_near:
        print(f"Near Duplicate: {url} - {other_url}. Similarity: {similarity:.2f}")
        return []
    
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = set()
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        
        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            absolute_url = urljoin(resp.url, urlparse(href).path)
            links.add(absolute_url)
    except Exception as e:
        print(f"Error extracting links from {url}: {e}")
        
    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        query_parts = parsed.query.lower().split('&')
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Basic sanity checks
        if len(query_parts) > 5:
            return False
        if len(url) > 300:
            return False
        if parsed.path.count('/') > 10:
            return False
        if len(parsed.query) > 200:
            return False

        # Calendar & pagination traps
        # year/month/day traps
        if re.search(r'\d{4}[-/]\d{2}[-/]\d{2}', url):
            return False
        # year/month traps
        if re.search(r'\d{4}[-/]\d{2}', url): # year/month
            return False
        pagination_patterns = ('page=', 'start=', 'offset=')
        if any(part.startswith(p) and re.search(r'\d+', part) for p in pagination_patterns for part in query_parts):
            return False

        # Session/token traps
        trap_params = ('session=', 'sid=', 'token=', 'jsessionid=')
        if any(part.startswith(p) for p in trap_params for part in query_parts):
            return False

        # Download/action traps
        if "action=download" in parsed.query.lower():
            return False
        if any(part.startswith(('version=', 'do=', 'rev=')) for part in query_parts):
            return False

        # Path-based traps
        if any(trap in parsed.path.lower() for trap in ['diff', 'media', 'history']):
            return False

        # Suspicious file traps
        if re.search(r'\.(php|aspx|jsp)$', parsed.path.lower()):
            if any(key in parsed.query.lower() for key in ('id=', 'files=')):
                return False

        # Long hash (commit-like) traps
        if re.search(r'[a-fA-F0-9]{32,}', parsed.path) or re.search(r'[a-fA-F0-9]{32,}', parsed.query):
            return False

        # GitLab-specific traps
        gitlab_ban_paths = (
            '/forks', '/issues', '/starrers', '/merge_requests', '/pipelines',
            '/jobs', '/blame', '/tags', '/branches', '/commits', '/repository',
            '/import', '/activity'
        )
        if '/-/' in url:
            if url.rstrip('/').endswith(gitlab_ban_paths):
                return False
            if parsed.path.endswith('/compare'):
                return False
            if re.search(r'/(commit|tree|compare)/[a-fA-F0-9]{10,}', parsed.path):
                return False
            if parsed.path.startswith('/-/commit') and any(p in parsed.query.lower() for p in ['view=', 'expanded=']):
                return False

        # Swiki traps (index loops)
        if 'idx=' in parsed.query.lower():
            return False
        
        # Skip tel and mail links
        # path of tel links like (949) xxx-xxxx
        # path of mail links like xxx@xxx.uci.edu
        if parsed.scheme in ['tel', 'mailto']:
            return False
        
        PHONE_RE = re.compile(r'\(\d{3}\)\s?\d{3}-\d{4}')
        EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}')
        path_and_query = parsed.path + "?" + parsed.query
        if PHONE_RE.search(path_and_query) or EMAIL_RE.search(path_and_query):
            return False

        domain = parsed.netloc.lower()
        if not (domain.endswith(".ics.uci.edu") or
                domain.endswith(".cs.uci.edu") or
                domain.endswith(".informatics.uci.edu") or
                domain.endswith(".stat.uci.edu") or
                domain == "today.uci.edu"):
            return False
        
        if domain == "today.uci.edu":
            if not parsed.path.startswith("/department/information_computer_sciences"):
                return False
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

# helper functions
def extract_visible_text(resp):
    soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    for script in soup(["script", "style"]):
        script.decompose()
    visible_text = soup.get_text(separator=" ", strip=True)
    return re.sub(r'\s+', ' ', visible_text)

def get_hash(text):
    return hashlib.sha1(text.encode('utf-8')).hexdigest()

def is_exact_duplicate(text):
    text_hash = get_hash(text)
    if text_hash in seen_hashes:
        return True
    seen_hashes.add(text_hash)
    return False

def get_shingles(text, k=SHINGLE_SIZE):
    words = text.split()
    return set(' '.join(words[i:i + k]) for i in range(len(words) - k + 1))

def jaccard_similarity(set1, set2):
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)

def is_near_duplicate(url, text):
    new_shingles = get_shingles(text)
    with seen_shingles_lock:
        for other_url, shingles in seen_shingles.items():
            similarity = jaccard_similarity(new_shingles, shingles)
            if similarity >= NEAR_DUPLICATE_THRESHOLD:
                return True, other_url, similarity
        seen_shingles[url] = new_shingles

    return False, None, 0.0
