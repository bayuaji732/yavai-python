# yavai/utils/package_manager.py

import subprocess
import sys
import logging

logger = logging.getLogger(__name__)

class PackageManager:
    def install(self, package_name):
        """Install a package via pip."""
        cmd = [sys.executable, "-m", "pip", "install"] + package_name.split()
        self._run(cmd)

    def uninstall(self, package_name):
        """Uninstall a package via pip."""
        cmd = [sys.executable, "-m", "pip", "uninstall"] + package_name.split()
        self._run(cmd)

    def upgrade(self, package_name):
        """Upgrade a package via pip."""
        cmd = [sys.executable, "-m", "pip", "upgrade"] + package_name.split()
        self._run(cmd)

    def list(self):
        """Lists all installed packages."""
        cmd = [sys.executable, "-m", "pip", "list"]
        self._run(cmd)

    def show(self, package_name):
        """Shows detailed information for a specific package."""
        cmd = [sys.executable, "-m", "pip", "show"] + package_name.split()
        self._run(cmd)

    def _run(self, cmd):
        subprocess.check_call(cmd)

# Create a singleton instance
pkg_manager = PackageManager()