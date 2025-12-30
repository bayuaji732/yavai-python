"""
YAVAI Python Library
Public API surface
"""

# ============================================================
# Internal shared context (NO io imports here)
# ============================================================

from yavai._context import api as _api
from yavai._context import tracker as _tracker
from yavai._context import jdbc as _jdbc
from yavai._context import sftp as _sftp

from yavai.datasets.api import DatasetAPI
from yavai.tracking.mlflow_wrapper import MLflowWrapper
from yavai.utils.package_manager import pkg_manager


# ============================================================
# Dataset / Metadata Browsing API
# ============================================================

def browse_dataset(dataset_id: str):
    """Browse dataset contents."""
    return _api.browse_dataset(dataset_id)


def browse_file(file_id: str):
    """Browse file metadata."""
    return _api.browse_file(file_id)


def browse_modelzoo(modelzoo_id: str):
    """Browse modelzoo metadata."""
    return _api.browse_modelzoo(modelzoo_id)


def get_table_preview(dataset_id: str, table_name: str):
    """Get preview of a JDBC table."""
    return _api.get_table_preview(dataset_id, table_name)


# ============================================================
# Experiment Tracking (MLflow)
# ============================================================

def set_experiment(experiment_name: str, **kwargs):
    return _tracker.set_experiment(experiment_name, **kwargs)


def get_experiment(experiment_id: str):
    return _tracker.get_experiment(experiment_id)


def get_experiment_by_name(experiment_name: str):
    return _tracker.get_experiment_by_name(experiment_name)


def start_run(run_name: str = None, **kwargs):
    _tracker.configure_aws_credentials()
    return _tracker.start_run(run_name=run_name, **kwargs)


def end_run(status: str = "FINISHED"):
    return _tracker.end_run(status=status)


def active_run():
    return _tracker.active_run()


def get_run(run_id: str):
    return _tracker.get_run(run_id)


def log_metric(key: str, value: float, step: int = None):
    return _tracker.log_metric(key, value, step)


def log_metrics(metrics: dict, step: int = None):
    return _tracker.log_metrics(metrics, step)


def log_param(key: str, value):
    return _tracker.log_param(key, value)


def log_params(params: dict):
    return _tracker.log_params(params)


def set_tag(key: str, value):
    return _tracker.set_tag(key, value)


def set_tags(tags: dict):
    return _tracker.set_tags(tags)


def log_text(text: str, artifact_file: str):
    return _tracker.log_text(text, artifact_file)


def log_artifact(local_path: str, artifact_path: str = None):
    return _tracker.log_artifact(local_path, artifact_path)


def log_artifacts(local_dir: str, artifact_path: str = None):
    return _tracker.log_artifacts(local_dir, artifact_path)


def log_dict(dictionary: dict, artifact_file: str):
    return _tracker.log_dict(dictionary, artifact_file)


def log_figure(figure, artifact_file: str):
    return _tracker.log_figure(figure, artifact_file)


def log_image(image, artifact_file: str):
    return _tracker.log_image(image, artifact_file)


def log_model(model, artifact_path: str, **kwargs):
    return _tracker.log_model(model, artifact_path, **kwargs)


def set_tracking_uri(uri: str):
    return _tracker.set_tracking_uri(uri)


def get_tracking_uri():
    return _tracker.get_tracking_uri()


def is_tracking_uri_set():
    return _tracker.is_tracking_uri_set()


# ============================================================
# System Utilities
# ============================================================

pkg = pkg_manager
jdbc = _jdbc
sftp = _sftp


# ============================================================
# IO Shortcuts (imported LAST to avoid cycles)
# ============================================================

from yavai.io import readers, media  # noqa: E402

read_csv = readers.read_csv
read_excel = readers.read_excel
read_sav = readers.read_sav
read_json = readers.read_json
read_file = readers.read_file

open_image = media.open_image
open_audio = media.open_audio
open_video = media.open_video
read_audio = media.read_audio
read_video = media.read_video


# ============================================================
# Metadata
# ============================================================

__version__ = "0.1.0"

__all__ = [
    # Core
    "DatasetAPI",
    "MLflowWrapper",

    # Dataset browsing
    "browse_dataset",
    "browse_file",
    "browse_modelzoo",
    "get_table_preview",

    # Experiment tracking
    "set_experiment",
    "get_experiment",
    "get_experiment_by_name",
    "start_run",
    "end_run",
    "active_run",
    "get_run",
    "log_metric",
    "log_metrics",
    "log_param",
    "log_params",
    "set_tag",
    "set_tags",
    "log_text",
    "log_artifact",
    "log_artifacts",
    "log_dict",
    "log_figure",
    "log_image",
    "log_model",
    "set_tracking_uri",
    "get_tracking_uri",
    "is_tracking_uri_set",

    # Utilities
    "pkg",
    "jdbc",
    "sftp",

    # IO
    "read_csv",
    "read_excel",
    "read_sav",
    "read_json",
    "read_file",
    "open_image",
    "open_audio",
    "open_video",
    "read_audio",
    "read_video",
]
