# yavai/_context.py

"""
Internal shared runtime context for YAVAI.

This module owns all long-lived singleton objects used across the package.
It must NOT import from yavai.io or any other module that depends on this context,
otherwise circular imports will occur.
"""

from yavai.datasets.api import DatasetAPI
from yavai.tracking.mlflow_wrapper import MLflowWrapper
from yavai.connections.jdbc import JDBC
from yavai.connections.sftp import SFTPClient


# ============================================================
# Core shared singletons
# ============================================================

api = DatasetAPI()
tracker = MLflowWrapper()

# Optional / infrastructure clients
jdbc = JDBC()
sftp = SFTPClient()
