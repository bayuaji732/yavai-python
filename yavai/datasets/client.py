"""YAVAI API Client for making HTTP requests to YAVAI services."""

import getpass
import json
import os
import warnings
from typing import Dict, List, Optional

import furl
import requests
from urllib3.exceptions import InsecureRequestWarning

from yavai import config

warnings.filterwarnings("ignore", category=InsecureRequestWarning)


class YAVAIClient:
    """Client for interacting with YAVAI API endpoints."""
    
    def __init__(self):
        self._session = requests.Session()
        self._username = getpass.getuser()

    def request(
        self,
        method: str,
        paths: List[str],
        base_paths: Optional[List[str]] = None,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        data: Optional[Dict] = None,
        is_download: bool = False,
        use_alt_base_url: bool = False,
        return_raw: bool = False  # Add this parameter
    ) -> Dict:
        """
        Make an HTTP request to YAVAI API.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            paths: URL path segments
            base_paths: Base path segments to prepend
            params: Query parameters
            headers: HTTP headers
            data: Request body data
            is_download: Whether this is a file download request
            use_alt_base_url: Use alternate base URL
            
        Returns:
            Response data as dictionary or download info
            
        Raises:
            requests.HTTPError: If request fails
        """
        url = self._build_url(paths, base_paths, use_alt_base_url)
        response = self._execute_request(method, url, headers, params, data)
        
        if is_download:
            return self._handle_download(response)

        if return_raw:  # Add this handling
            return response.text
        
        return response.json()

    def _build_url(
        self, 
        paths: List[str], 
        base_paths: Optional[List[str]], 
        use_alt_base_url: bool
    ) -> str:
        """Build complete URL from components."""
        base_url = config.API_BASE_URL2 if use_alt_base_url else config.API_BASE_URL
        url = furl.furl(base_url)
        url.path.segments = (base_paths or []) + paths
        return str(url)

    def _execute_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict],
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> requests.Response:
        """Execute HTTP request with error handling."""
        response = self._session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=json.dumps(data) if data else None,
            verify=False
        )
        response.raise_for_status()
        return response

    def _handle_download(self, response: requests.Response) -> Dict:
        """Handle file download response."""
        username = getpass.getuser()
        filename = f"/home/{username}/downloaded_file.zip"
        with open(filename, 'wb') as f:
            f.write(response.content)
        return {
            'statusCode': 200, 
            'status': 'OK', 
            'filename': filename
        }