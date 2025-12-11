import time
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from time import sleep
from random import uniform


from shared.utils.helper import project_path
from shared.utils.log_setup import setup_logging

# ==============================================================================
# LAYER 1: Base Source - Core interface for ALL acquisition sources
# ==============================================================================

class BaseSource(ABC):
    """
    Abstract base class for ALL acquisition sources.
    """
    
    def __init__(
        self,
        source_id: str,
        **config
    ):
        """
        Initialize the base source.
        """
        self.source_id = source_id
        self.logger = setup_logging(name = source_id)
        self.config = config
        self.stats = {
            'items_fetched': 0,
            'items_validated': 0,
            'items_failed': 0,
            'errors': [],
            'session_start': None,
            'session_end': None,
        }
    
    def _rate_limit_delay(self):
        return sleep(uniform(2, 3))
        
    def _track_error(self, error_type: str, message: str, context: str = ""):
        error_record = {
            'type': error_type,
            'message': message,
            'context': context,
            'timestamp': datetime.now().isoformat(),
        }
        self.stats['errors'].append(error_record)
        self.logger.error(f"{error_type}: {message} {context}")
    
    def start_session(self):
        self.stats['session_start'] = datetime.now()
        self.logger.info(f"Starting collection session for {self.source_id}")
    
    def end_session(self):
        """Mark the end of a session and log summary statistics."""
        self.stats['session_end'] = datetime.now()
        self.logger.info(f"Session complete for {self.source_id}")

    
    def get_stats(self) -> Dict[str, Any]:
        """Get current session statistics."""
        return {**self.stats}
    
    def reset_stats(self):
        self.stats = {
            'items_fetched': 0,
            'items_validated': 0,
            'items_failed': 0,
            'errors': [],
            'session_start': None,
            'session_end': None,
        }
    
    # =========================================================================
    # Abstract methods - MUST be implemented by all sources
    # =========================================================================
    
    @abstractmethod
    def fetch(self, **params) -> List[Dict[str, Any]]:
        """
        Fetch raw data from the source.
        
        Args:
            **params: Source-specific parameters
            
        Returns:
            List of raw data items
        """
        pass
    
    @abstractmethod
    def parse(self, raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a raw item into standardized format.
        
        Args:
            raw_item: Raw data item
            
        Returns:
            Parsed item or None if parsing fails
        """
        pass
    
    @abstractmethod
    def validate(self, item: Dict[str, Any]) -> bool:
        """
        Validate that an item has required fields and valid data.
        
        Args:
            item: Parsed item
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    # =========================================================================
    # High-level workflow
    # =========================================================================
    
    def collect(self, **fetch_params) -> List[Dict[str, Any]]:
        self.start_session()
        
        try:
            # Fetch raw data
            raw_items = self.fetch(**fetch_params)
            self.stats['items_fetched'] = len(raw_items)
            self.logger.info(f"Fetched {len(raw_items)} raw items")
            
            # Parse and validate
            validated_items = []
            for raw_item in raw_items:
                try:
                    # Parse
                    parsed = self.parse(raw_item)
                    if not parsed:
                        self.stats['items_failed'] += 1
                        continue
                    
                    # Validate
                    if self.validate(parsed):
                        validated_items.append(parsed)
                        self.stats['items_validated'] += 1
                    else:
                        self.stats['items_failed'] += 1
                        
                except Exception as e:
                    self.stats['items_failed'] += 1
                    self._track_error('processing_error', str(e), str(raw_item)[:100])
            
            self.logger.info(f"Validated {len(validated_items)} items")
            return validated_items
            
        finally:
            self.end_session()
    

# ==============================================================================
# LAYER 2: HTTP Source - Adds HTTP functionality to BaseSource
# ==============================================================================


class BaseHTTPSource(BaseSource):
    """
    Base class for HTTP-based acquisition sources.
    
    Extends BaseSource with:
    - HTTP session with automatic retries
    - Rate limiting
    - Proxy support
    - Convenient HTTP methods (get, post, get_json, etc.)
    """
    
    def __init__(
        self,
        source_id: str,
        user_agent: str,
        base_url: Optional[str] = None,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
        timeout: int = 30,
        proxy_config: Optional[Dict[str, str]] = None,
        log_dir: Optional[str] = None,
        **config
    ):
        # Initialize parent BaseSource
        super().__init__(source_id, log_dir, **config)
        
        self.user_agent = user_agent
        self.base_url = base_url.rstrip('/') if base_url else None
        self.timeout = timeout
        self.proxy_config = proxy_config
        
        # HTTP-specific stats
        self.stats.update({
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'errors_by_type': {},
        })
        
        # Setup HTTP session
        self.session = self._setup_session(max_retries, retry_backoff)
        
        # Configure proxies if provided
        if proxy_config:
            self._configure_proxies(proxy_config)
        
        # Rate limiting
        self._last_request_time = 0
    
    def _setup_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        """Configure requests session with retry strategy."""
        session = requests.Session()
        
        # Configure automatic retries
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            raise_on_status=False,
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })
        
        return session
    
    def _configure_proxies(self, proxy_config: Dict[str, str]):
        """Configure proxy settings for the session."""
        username = proxy_config.get('username')
        password = proxy_config.get('password')
        host = proxy_config.get('host', 'proxy.webshare.io')
        port = proxy_config.get('port', '80')
        
        if not username or not password:
            self.logger.warning("Proxy credentials missing, skipping proxy setup")
            return
        
        # Build proxy URL with authentication
        proxy_url = f"http://{username}:{password}@{host}:{port}"
        
        self.session.proxies = {
            'http': proxy_url,
            'https': proxy_url,
        }
        
        self.logger.info(f"Configured proxy: {username}@{host}:{port}")

    
    def _track_http_error(self, error_type: str):
        """Track HTTP error types."""
        self.stats['errors_by_type'][error_type] = \
            self.stats['errors_by_type'].get(error_type, 0) + 1
    
    def _build_url(self, url: str) -> str:
        """Build full URL from base_url and path."""
        if url.startswith(('http://', 'https://')):
            return url
        if self.base_url:
            return f"{self.base_url}/{url.lstrip('/')}"
        return url
    
    def request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Optional[requests.Response]:
        """
        Make an HTTP request with error handling and rate limiting.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL or path
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object or None
        """
        url = self._build_url(url)
        self._enforce_rate_limit()
        self.stats['total_requests'] += 1
        
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        
        try:
            self.logger.debug(f"{method} {url}")
            
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            
            self.stats['successful_requests'] += 1
            self.logger.debug(f"✓ {method} {url} - {response.status_code}")
            
            return response
            
        except requests.exceptions.HTTPError as e:
            self.stats['failed_requests'] += 1
            status = e.response.status_code if e.response else 'unknown'
            self._track_http_error(f'http_{status}')
            self._track_error('http_error', f"HTTP {status}", url)
            return None
            
        except requests.exceptions.Timeout as e:
            self.stats['failed_requests'] += 1
            self._track_http_error('timeout')
            self._track_error('timeout', str(e), url)
            return None
            
        except requests.exceptions.ConnectionError as e:
            self.stats['failed_requests'] += 1
            self._track_http_error('connection_error')
            self._track_error('connection_error', str(e), url)
            return None
            
        except requests.exceptions.RequestException as e:
            self.stats['failed_requests'] += 1
            self._track_http_error('request_exception')
            self._track_error('request_exception', str(e), url)
            return None
        
        except Exception as e:
            self.stats['failed_requests'] += 1
            self._track_http_error('unexpected_error')
            self._track_error('unexpected_error', str(e), url)
            return None
    
    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Make a GET request."""
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Make a POST request."""
        return self.request('POST', url, **kwargs)
    
    def get_json(self, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make a GET request and parse JSON response."""
        response = self.get(url, **kwargs)
        if response:
            try:
                return response.json()
            except ValueError as e:
                self._track_error('json_parse_error', str(e), url)
                return None
        return None
    
    def get_text(self, url: str, **kwargs) -> Optional[str]:
        """Make a GET request and return text content."""
        response = self.get(url, **kwargs)
        return response.text if response else None
    
    def end_session(self):
        """Override to add HTTP stats to session summary."""
        super().end_session()
        
        if self.stats['session_start']:
            success_rate = 0
            if self.stats['total_requests'] > 0:
                success_rate = (self.stats['successful_requests'] 
                              / self.stats['total_requests'] * 100)
            
            self.logger.info(f"  HTTP Requests: {self.stats['total_requests']} "
                           f"({success_rate:.1f}% success)")
            
            if self.stats['errors_by_type']:
                self.logger.warning(f"  HTTP Errors: {self.stats['errors_by_type']}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Override to close HTTP session."""
        super().__exit__(exc_type, exc_val, exc_tb)
        self.session.close()




if __name__ == "__main__":
    pass