"""Dataset Management API for YAVAI platform."""

from typing import Dict
import pandas as pd

from yavai import config
from yavai.datasets.client import YAVAIClient


class DatasetAPI:
    """API client for dataset management operations."""
    
    # API version paths
    V1_LIB = ["dataset-management", "api", "v1", "lib"]
    V1 = ["dataset-management", "api", "v1"]
    V2 = ["dataset-management", "api", "v2"]
    
    def __init__(self):
        self._client = YAVAIClient()

    def get_file_path(self, file_id: str) -> str:
        """
        Get S3A path for a file.
        
        Args:
            file_id: Unique file identifier
            
        Returns:
            S3A path string
        """
        response = self._client.request(
            "GET", 
            ["files", file_id, "s3a-path"], 
            base_paths=self.V1_LIB
        )
        return response.get("data")

    def browse_dataset(self, dataset_id: str) -> Dict:
        """
        Browse dataset contents.
        
        Args:
            dataset_id: Unique dataset identifier
            
        Returns:
            Dataset metadata and contents
        """
        response = self._client.request(
            "GET", 
            ["datasets", dataset_id, "browse"], 
            base_paths=self.V1_LIB
        )
        return response.get("data")

    def browse_file(self, file_id: str) -> Dict:
        """
        Browse file metadata.
        
        Args:
            file_id: Unique file identifier
            
        Returns:
            File metadata
        """
        response = self._client.request(
            "GET", 
            ["files", file_id, "browse"], 
            base_paths=self.V1_LIB
        )
        return response.get("data")

    def download_dataset(self, dataset_id: str) -> Dict:
        """
        Download complete dataset as ZIP.
        
        Args:
            dataset_id: Unique dataset identifier
            
        Returns:
            Download information including filename
        """
        headers = {"Authorization": f"Bearer {config.TOKEN}"}
        return self._client.request(
            "GET", 
            ["datasets", dataset_id, "download"], 
            base_paths=self.V2, 
            headers=headers, 
            is_download=True
        )

    def browse_modelzoo(self, modelzoo_id: str) -> Dict:
        """
        Browse model zoo contents.
        
        Args:
            modelzoo_id: Unique model zoo identifier
            
        Returns:
            Model zoo metadata
        """
        response = self._client.request(
            "GET", 
            ["list-file-modelZoo", modelzoo_id], 
            use_alt_base_url=True
        )
        return response.get("data")

    def browse_jdbc_tables(self, dataset_id: str) -> Dict:
        """
        Browse JDBC database tables.
        
        Args:
            dataset_id: Unique dataset identifier
            
        Returns:
            Available tables metadata
        """
        response = self._client.request(
            "GET", 
            ["jdbcdisplay", "table", dataset_id], 
            base_paths=self.V1
        )
        return response.get("data")

    def get_table_preview(self, dataset_id: str, table_name: str) -> pd.DataFrame:
        """
        Get preview of a JDBC table.
        
        Args:
            dataset_id: Unique dataset identifier
            table_name: Name of the table to preview
            
        Returns:
            DataFrame containing table preview data
        """
        params = {"tableName": table_name}
        response = self._client.request(
            "GET", 
            ["jdbcdisplay", "preview", dataset_id], 
            base_paths=self.V2, 
            params=params
        )
        return pd.DataFrame(response.get("data"))