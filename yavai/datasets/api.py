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
    V3 = ["api", "v1", "feature-groups"]
    V4 = ["api", "v1", "training-datasets"]
    
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

    # Feature Groups
    def create_feature_group(self, app_name: str, app_token: str, feature_group: Dict) -> Dict:
        """
        Create a new feature group.
        
        Args:
            app_name: Application name
            app_token: Application token
            feature_group: Feature group configuration
            
        Returns:
            Created feature group data
        """
        data = {
            "app_name": app_name,
            "app_token": app_token,
            "feature_group": feature_group
        }
        response = self._client.request(
            "POST",
            ["feature-groups"],
            base_paths=self.V3,
            data=data
        )
        return response.get("data")

    def read_feature_group(self, app_name: str, feature_group: Dict) -> pd.DataFrame:
        """
        Read feature group data.
        
        Args:
            app_name: Application name
            feature_group: Feature group configuration
            
        Returns:
            DataFrame containing feature group preview
        """
        data = {
            "app_name": app_name,
            "feature_group": feature_group
        }
        response = self._client.request(
            "POST",
            ["feature-groups", "preview"],
            base_paths=self.V3,
            data=data
        )
        return pd.DataFrame(response.get("data"))

    def delete_feature_group(self, app_name: str, app_token: str, feature_group: Dict) -> Dict:
        """
        Delete a feature group.
        
        Args:
            app_name: Application name
            app_token: Application token
            feature_group: Feature group configuration
            
        Returns:
            Deletion status
        """
        data = {
            "app_name": app_name,
            "app_token": app_token,
            "feature_group": feature_group
        }
        response = self._client.request(
            "POST",
            ["feature-groups", "delete"],
            base_paths=self.V3,
            data=data
        )
        return response.get("data")

    # Training Datasets
    def create_training_dataset(self, app_name: str, app_token: str, 
                               training_dataset: Dict, data: Dict) -> Dict:
        """
        Create a new training dataset.
        
        Args:
            app_name: Application name
            app_token: Application token
            training_dataset: Training dataset configuration
            data: Training dataset DTO object
            
        Returns:
            Created training dataset data
        """
        request_data = {
            "app_name": app_name,
            "app_token": app_token,
            "training_dataset": training_dataset,
            "data": data
        }
        response = self._client.request(
            "POST",
            ["training-datasets"],
            base_paths=self.V4,
            data=request_data
        )
        return response.get("data")

    def read_training_dataset(self, app_name: str, training_dataset: Dict) -> pd.DataFrame:
        """
        Read training dataset data.
        
        Args:
            app_name: Application name
            training_dataset: Training dataset configuration
            
        Returns:
            DataFrame containing training dataset preview
        """
        data = {
            "app_name": app_name,
            "training_dataset": training_dataset
        }
        response = self._client.request(
            "POST",
            ["training-datasets", "preview"],
            base_paths=self.V4,
            data=data
        )
        return pd.DataFrame(response.get("data"))

    def delete_training_dataset(self, app_name: str, app_token: str, 
                                training_dataset: Dict) -> Dict:
        """
        Delete a training dataset.
        
        Args:
            app_name: Application name
            app_token: Application token
            training_dataset: Training dataset configuration
            
        Returns:
            Deletion status
        """
        data = {
            "app_name": app_name,
            "app_token": app_token,
            "training_dataset": training_dataset
        }
        response = self._client.request(
            "POST",
            ["training-datasets", "delete"],
            base_paths=self.V4,
            data=data
        )
        return response.get("data")