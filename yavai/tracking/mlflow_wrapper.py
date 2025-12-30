# yavai/tracking/mlflow_wrapper.py

import getpass
import logging
import os
from typing import Any, Dict, Optional, Union
import warnings

import mlflow
import numpy as np
import requests
from PIL import Image
from matplotlib.figure import Figure
from plotly.graph_objs import Figure as PlotlyFigure

from yavai import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLflowWrapper:
    """Wrapper for MLflow experiment tracking operations."""
    
    def __init__(self):
        self._username = getpass.getuser().lower()

    def configure_aws_credentials(self) -> None:
        """Set AWS environment variables for S3 access."""
        if all([
            config.AWS_ACCESS_KEY_ID,
            config.AWS_SECRET_ACCESS_KEY,
            config.MLFLOW_S3_ENDPOINT_URL
        ]):
            os.environ["AWS_ACCESS_KEY_ID"] = config.AWS_ACCESS_KEY_ID
            os.environ["AWS_SECRET_ACCESS_KEY"] = config.AWS_SECRET_ACCESS_KEY
            os.environ["MLFLOW_S3_ENDPOINT_URL"] = config.MLFLOW_S3_ENDPOINT_URL
            logger.info("AWS credentials configured successfully")
        else:
            logger.warning("AWS credentials incomplete, skipping configuration")

    # Run Management
    def start_run(self, **kwargs) -> mlflow.ActiveRun:
        """Start a new MLflow run."""
        return mlflow.start_run(**kwargs)

    def end_run(self, status: str = 'FINISHED') -> None:
        """End the active MLflow run."""
        mlflow.end_run(status)

    def active_run(self):
        return mlflow.active_run()

    def get_run(self, run_id: str):
        return mlflow.get_run(run_id)
    
    def get_experiment(self, experiment_id: str):
        return mlflow.get_experiment(experiment_id)

    def get_experiment_by_name(self, name: str):
        return mlflow.get_experiment_by_name(name)

    # Logging Methods
    def log_param(self, key: str, value: Any) -> None:
        """Log a single parameter."""
        mlflow.log_param(key, value)

    def log_params(self, params: Dict[str, Any]) -> None:
        """Log multiple parameters."""
        mlflow.log_params(params)

    def log_metric(self, key: str, value: float, step: Optional[int] = None) -> None:
        """Log a single metric."""
        mlflow.log_metric(key, value, step=step)

    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None) -> None:
        """Log multiple metrics."""
        mlflow.log_metrics(metrics, step=step)

    def log_artifact(self, file_path: str) -> None:
        """Log a single artifact file."""
        mlflow.log_artifact(file_path)

    def log_artifacts(self, dir_path: str) -> None:
        """Log all files in a directory as artifacts."""
        mlflow.log_artifacts(dir_path)

    def log_dict(self, dictionary: Dict[str, Any], artifact_path: str) -> None:
        """Log a dictionary as a JSON artifact."""
        mlflow.log_dict(dictionary, artifact_path)

    def log_text(self, text: str, artifact_path: str) -> None:
        """Log text content as an artifact."""
        mlflow.log_text(text, artifact_path)

    def log_figure(self, figure: Union[Figure, PlotlyFigure], file_name: str) -> None:
        """Log a matplotlib or plotly figure."""
        mlflow.log_figure(figure, file_name)

    def log_image(self, image: Union[np.ndarray, Image.Image], file_name: str) -> None:
        """Log an image (numpy array or PIL Image)."""
        mlflow.log_image(image, file_name)

    def log_model(self, model, name: str, **kwargs):
        """
        Logs a model.
        MLflow 3.x Change: 'artifact_path' is deprecated. We now use 'name'.
        """
        self.configure_aws_credentials()
        
        # Check if the user is using sklearn, pytorch, etc.
        # This is a simplified example; you might want to inspect the model type
        if "sklearn" in str(type(model)):
            # In MLflow 3, use 'name' instead of 'artifact_path'
            mlflow.sklearn.log_model(model, artifact_path=name, **kwargs)
        else:
            # Fallback or generic python model
            mlflow.pyfunc.log_model(python_model=model, artifact_path=name, **kwargs)

    # Tags
    def set_tag(self, key: str, value: str) -> None:
        """Set a single tag."""
        mlflow.set_tag(key, value)

    def set_tags(self, tags: Dict[str, str]) -> None:
        """Set multiple tags."""
        mlflow.set_tags(tags)

    # Tracking URI Management
    def set_tracking_uri(self, uri: str) -> None:
        """
        Configure MLflow tracking URI.
        
        Args:
            uri: Tracking URI or "yavai" for automatic configuration
        """
        self.configure_aws_credentials()

        tracking_uri = self._resolve_tracking_uri(uri)
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.autolog()
        logger.info(f"Tracking URI set to: {tracking_uri}")

    def get_tracking_uri(self) -> str:
        """Get the current tracking URI."""
        return mlflow.get_tracking_uri()

    def is_tracking_uri_set(self) -> bool:
        """Check if tracking URI has been configured."""
        return mlflow.is_tracking_uri_set()

    def _resolve_tracking_uri(self, uri: str) -> str:
        """Resolve tracking URI from input or fetch from API."""
        if uri.lower() != "yavai":
            return uri

        username = getpass.getuser()

        # Special-case Jupyter / jovyan
        if username == "jovyan":
            logger.warning("Running as jovyan user; User must declare the URI manually")
            return None

        try:
            url = f"{config.API_URL2}{username}"
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()
            if data.get("status") == 200 and "url_mlflow" in data:
                return data["url_mlflow"]

            raise RuntimeError("Username does not exist in YAVAI system")

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch MLflow URI: {e}")

    # Experiment Management
    def set_experiment(self, experiment_name: str, **kwargs) -> None:
        """
        Set the active experiment.
        
        Args:
            experiment_name: Name of the experiment
            **kwargs: Additional arguments for set_experiment
        """
        current_uri = mlflow.get_tracking_uri()
        
        if 'file:///' in current_uri:
            logger.error("Invalid tracking URI. Please set a valid tracking URI first.")
            return

        mlflow.set_experiment(experiment_name, **kwargs)
        logger.info(f"Active experiment set to: {experiment_name}")
        
        if self._username != "jovyan":
            self._update_experiment_status(experiment_name)

    def _update_experiment_status(self, experiment_name: str) -> None:
        """Update experiment status via YAVAI API."""
        try:
            url = f"{config.API_URL}/{experiment_name}"
            params = {"username": self._username, "status": "true"}
            requests.put(url, params=params)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to update experiment status: {e}")