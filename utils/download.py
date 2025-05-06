import requests
import cbor
import time

from utils.response import Response

def download(url: str, config, logger=None) -> Optional[Response]:
    """
    Download a URL using the Spacetime cache server.
    The function first sends a HEAD request to the URL to check if it is valid.
    If the HEAD request is successful, it then sends a GET request to the Spacetime
    """
    # step 1: check if the URL is valid
    try:
        head_start = time.perf_counter()
        head_resp = requests.head(
            url,
            allow_redirects=True,
            headers={"User-Agent": config.user_agent},
            timeout=3.0
        )
        head_latency = time.perf_counter() - head_start
        if logger:
            logger.debug(f"HEAD {head_latency:.3f}s | {url} "
                         f"| status {head_resp.status_code}")

        # Check if the HEAD request was successful
        if not (200 <= head_resp.status_code < 400):
            return Response({
                "error": "HEAD indicated non‑success",
                "status": head_resp.status_code,
                "url": url
            })

    except requests.exceptions.Timeout:
        if logger:
            logger.warning(f"HEAD timeout (>5 s) for {url}")
        return None
    except requests.exceptions.RequestException as exc:
        if logger:
            logger.warning(f"HEAD error for {url}: {exc}")
        return None

    # step 2: check if the URL is in the cache
    host, port = config.cache_server
    try:
        body_start = time.perf_counter()
        resp = requests.get(
            f"http://{host}:{port}/",
            params=[("q", url), ("u", config.user_agent)],
            timeout=(3.0, 5.0)   # connect_timeout = 3 s, read_timeout = 5 s
        )
        body_latency = time.perf_counter() - body_start
        if logger:
            logger.debug(f"CACHE {body_latency:.3f}s | {url} "
                         f"| cache_status {resp.status_code}")

    except requests.exceptions.Timeout:
        if logger:
            logger.warning(f"Cache fetch timeout (>5 s) for {url}")
        return None
    except requests.exceptions.RequestException as exc:
        if logger:
            logger.warning(f"Cache fetch error for {url}: {exc}")
        return None

    if resp and resp.content:
        try:
            return Response(cbor.loads(resp.content))
        except (EOFError, ValueError):
            if logger:
                logger.error(f"CBOR decode error for {url}")
            return None
        
    if logger:
        logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": getattr(resp, "status_code", None),
        "url": url
    })