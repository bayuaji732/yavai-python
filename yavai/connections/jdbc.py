# yavai/connections/jdbc.py

import logging
import os
import pandas as pd
import requests
from pathlib import Path
from typing import Optional, Dict

class JDBC:
    # Maven Central coordinates for common JDBC drivers
    JAR_REGISTRY = {
        "hive": {
            "group_id": "org.apache.hive",
            "artifact_id": "hive-jdbc",
            "version": "3.1.3",
            "driver_class": "org.apache.hive.jdbc.HiveDriver"
        },
        "postgresql": {
            "group_id": "org.postgresql",
            "artifact_id": "postgresql",
            "version": "42.7.1",
            "driver_class": "org.postgresql.Driver"
        },
        "mysql": {
            "group_id": "com.mysql",
            "artifact_id": "mysql-connector-j",
            "version": "8.2.0",
            "driver_class": "com.mysql.cj.jdbc.Driver"
        },
        "hadoop": {
            "group_id": "org.apache.hadoop",
            "artifact_id": "hadoop-common",
            "version": "3.3.6",
            "driver_class": None
        }
    }
    
    def __init__(self, jar_dir: Optional[str] = None, auto_download: bool = True):
        self._conn = None
        self._cursor = None
        self.auto_download = auto_download
        
        # Set JAR directory
        if jar_dir:
            self.jar_dir = Path(jar_dir)
        else:
            self.jar_dir = Path.home() / ".yavai" / "jars"
        
        # Create directory if it doesn't exist
        self.jar_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"JAR directory: {self.jar_dir}")

    def connect(self, url: str, user: str = None, password: str = None, kerberos: bool = False):
        # Lazy import
        try:
            import jaydebeapi
        except ImportError:
            raise ImportError("jaydebeapi is required. Install it with: pip install JayDeBeApi")

        # Determine required JARs based on URL
        required_jars = self._identify_required_jars(url)
        
        # Check and download missing JARs
        jar_paths = []
        for jar_type, jar_info in required_jars.items():
            jar_path = self._ensure_jar_exists(jar_type, jar_info)
            if jar_path:
                jar_paths.append(jar_path)
        
        if required_jars and not jar_paths:
            raise RuntimeError("No valid JARs found or downloaded")
        
        # Build classpath
        classpath = ":".join([str(p) for p in jar_paths])
        logging.info(f"Classpath: {classpath}")
        
        # Determine driver
        driver_name = self._get_driver_name(url)
        if not driver_name:
            raise ValueError(f"Could not determine driver for URL: {url}")
        
        # Connect
        try:
            if kerberos:
                self._conn = jaydebeapi.connect(driver_name, url, [], classpath)
            else:
                auth = [user, password] if user else []
                self._conn = jaydebeapi.connect(driver_name, url, auth, classpath)
                
            self._cursor = self._conn.cursor()
            logging.info("Connected via JDBC")
            return True
        except Exception as e:
            logging.error(f"JDBC Connection failed: {e}")
            raise

    def _identify_required_jars(self, url: str) -> Dict[str, dict]:
        """Identify which JARs are needed based on the connection URL."""
        required = {}
        
        url_lower = url.lower()
        
        if "hive" in url_lower:
            required["hive"] = self.JAR_REGISTRY["hive"]
            required["hadoop"] = self.JAR_REGISTRY["hadoop"]
        elif "postgresql" in url_lower:
            required["postgresql"] = self.JAR_REGISTRY["postgresql"]
        elif "mysql" in url_lower:
            required["mysql"] = self.JAR_REGISTRY["mysql"]
        
        return required

    def _ensure_jar_exists(self, jar_type: str, jar_info: dict) -> Optional[Path]:
        """
        Check if JAR exists, download if missing and auto_download is True.
        
        Args:
            jar_type: Type of JAR (e.g., 'hive', 'postgresql')
            jar_info: Dictionary with Maven coordinates
            
        Returns:
            Path to the JAR file or None if unavailable
        """
        # Construct expected JAR filename
        jar_filename = f"{jar_info['artifact_id']}-{jar_info['version']}.jar"
        jar_path = self.jar_dir / jar_filename
        
        # Check if JAR exists
        if jar_path.exists():
            logging.info(f"Found {jar_type} JAR: {jar_path}")
            return jar_path
        
        # If not found and auto_download is enabled, download it
        if self.auto_download:
            logging.warning(f"{jar_type} JAR not found at {jar_path}")
            logging.info(f"Attempting to download {jar_type} JAR...")
            
            if self._download_jar(jar_info, jar_path):
                return jar_path
            else:
                logging.error(f"Failed to download {jar_type} JAR")
                return None
        else:
            logging.error(f"{jar_type} JAR not found and auto_download is disabled")
            return None

    def _download_jar(self, jar_info: dict, destination: Path) -> bool:
        # Construct Maven Central URL
        group_path = jar_info['group_id'].replace('.', '/')
        artifact_id = jar_info['artifact_id']
        version = jar_info['version']
        
        url = (
            f"https://repo1.maven.org/maven2/"
            f"{group_path}/{artifact_id}/{version}/"
            f"{artifact_id}-{version}.jar"
        )
        
        try:
            logging.info(f"Downloading from: {url}")
            
            # Stream download with progress
            try:
                response = requests.get(url, stream=True, timeout=60)
            except Exception:
                return False
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(destination, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    chunk_size = 8192
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        downloaded += len(chunk)
                        f.write(chunk)
                        
                        # Simple progress indicator
                        percent = (downloaded / total_size) * 100
                        if downloaded % (chunk_size * 100) == 0:
                            logging.info(f"Download progress: {percent:.1f}%")
            
            logging.info(f"Successfully downloaded to {destination}")
            return True
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Download failed: {e}")
            # Clean up partial download
            if destination.exists():
                destination.unlink()
            return False

    def execute(self, query: str):
        """Execute a SQL query and return results as a DataFrame."""
        if not self._cursor:
            raise ConnectionError("Not connected. Call connect() first.")
        
        self._cursor.execute(query)
        results = self._cursor.fetchall()
        
        if self._cursor.description:
            cols = [d[0] for d in self._cursor.description]
            return pd.DataFrame(results, columns=cols)
        return None

    def close(self):
        """Close the database connection."""
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        logging.info("JDBC connection closed")

    @staticmethod
    def _get_driver_name(url: str) -> Optional[str]:
        """Determine the JDBC driver class based on the connection URL."""
        url_lower = url.lower()
        
        if "hive" in url_lower:
            return "org.apache.hive.jdbc.HiveDriver"
        elif "postgresql" in url_lower:
            return "org.postgresql.Driver"
        elif "mysql" in url_lower:
            return "com.mysql.cj.jdbc.Driver"
        
        return None

    def list_available_drivers(self):
        """List all available driver configurations."""
        return {k: v['driver_class'] for k, v in self.JAR_REGISTRY.items() if v['driver_class']}

    def add_custom_driver(self, name: str, group_id: str, artifact_id: str, 
                         version: str, driver_class: str):
        """
        Add a custom driver configuration.
        
        Args:
            name: Short name for the driver (e.g., 'oracle')
            group_id: Maven group ID
            artifact_id: Maven artifact ID
            version: Driver version
            driver_class: Fully qualified driver class name
        """
        self.JAR_REGISTRY[name] = {
            "group_id": group_id,
            "artifact_id": artifact_id,
            "version": version,
            "driver_class": driver_class
        }
        logging.info(f"Added custom driver: {name}")