# yavai/connections/sftp.py

import os
import paramiko

class SFTPClient:
    def __init__(self):
        self.ssh = None
        self.sftp = None

    def connect(self, hostname, username, password, port=22):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname, port=port, username=username, password=password)
        self.sftp = self.ssh.open_sftp()
        print(f"Connected to {hostname}")
        return self

    def close(self):
        if self.sftp: self.sftp.close()
        if self.ssh: self.ssh.close()

    # Allow usage: "with yavai.SFTP() as sftp:"
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def list_files(self, remote_path="."):
        return self.sftp.listdir(remote_path)

    def download(self, remote_path, local_path=None):
        local_path = local_path or os.path.basename(remote_path)
        self.sftp.get(remote_path, local_path)

    def upload(self, local_path, remote_path):
        self.sftp.put(local_path, remote_path)