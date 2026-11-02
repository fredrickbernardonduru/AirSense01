import httpx

def build_http_client(timeout_seconds: float = 30.0) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout_seconds),
        headers={"User-Agent": "AirSense/0.1 (East Africa air quality research project)"},
    )
