import paramiko
import os
import time
from stat import S_ISDIR, S_ISREG
from src.package import utils
from collections import deque

LOG_CONFIG_FILE_PATH: str = os.path.join('..', 'conf', 'logger.yaml')


class SFTPHandler:
    def __init__(self):
        self.logger = utils.get_logger_config(LOG_CONFIG_FILE_PATH, logger_name='handler')
        self.sftp = None
        self.ssh = None
        self.transport = None
        self.channel = None
        self.local_default_dir_path = None
        self.remote_default_dir_path = None

    def set_default_path(self, local_default_dir_path: str = None, remote_default_dir_path: str = None) -> None:
        self.local_default_dir_path: str = local_default_dir_path
        self.remote_default_dir_path: str = remote_default_dir_path
        self.logger.info('local/remote desult path set completely(local : {0}, remote : {1})'.format(local_default_dir_path, remote_default_dir_path))

    def create_sftp_session(self, host, username, password, port) -> None:
        self.transport = paramiko.Transport(host, port)
        self.transport.connect(username=username, password=password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def create_ssh_session(self, host, username, password, port) -> None:
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(host, username=username, password=password, port=port)

    def create_command_channel(self) -> None:
        self.logger.info('Attempting to create an command input channel creat')
        self.channel = self.ssh.invoke_shell()
        self.logger.info('Command input channel created successfully')

    def send_shell_command(self, command: str) -> None:
        self.channel.send(command + '\n')
        time.sleep(0.1)

    def check_exist(self, path: str, check_point: str) -> bool:
        check_exist: bool = False

        if check_point == 'remote':
            try:
                self.sftp.stat(path)
                check_exist = True
            except IOError:
                return check_exist
        elif check_point == 'local':
            check_exist = os.path.exists(path)

        return check_exist

    def get_filesize(self, path: str, check_point: str) -> int:
        filesize: int = 0

        if check_point == 'remote':
            filesize = self.sftp.stat(path).st_size
        elif check_point == 'local':
            filesize = os.stat(path).st_size

        return filesize

    def mkdir(self, path: str, to: str) -> None:
        if to == 'remote':
            self.sftp.mkdir(path)
        elif to == 'local':
            os.mkdir(path)

    def download(self, remote_file_path: str, local_file_path: str) -> None:
        try:
            self.sftp.get(remote_file_path, local_file_path)
        except Exception as e:
            raise e

    def check_file_transfer_integrity(self, local_file_path: str, remote_file_path: str, purpose: str) -> bool:
        integrity: bool = False
        check_file_exist: bool = False

        if purpose == 'download':
            check_file_exist = self.check_exist(local_file_path, check_point='local')
        elif purpose == 'upload':
            check_file_exist = self.check_exist(remote_file_path, check_point='remote')

        local_filesize: int = self.get_filesize(local_file_path, check_point='local')
        remote_filesize: int = self.get_filesize(remote_file_path, check_point='remote')

        if check_file_exist and local_filesize == remote_filesize:
            integrity = True

        return integrity

    def walk_dir(self, top: str, on: str) -> dict:
        tree: dict = {}

        if on == 'remote':
            paths_to_explore = deque([top])
            while len(paths_to_explore) > 0:
                top = paths_to_explore.popleft()

                if top not in tree:
                    tree[top] = {'dirs': [], 'files': []}

                for attr in self.sftp.listdir_attr(top):
                    path = utils.make_linux_path(top, attr.filename)

                    if S_ISDIR(attr.st_mode):
                        tree[top]['dirs'].append(attr.filename)
                        paths_to_explore.append(path)
                    elif S_ISREG(attr.st_mode):
                        tree[top]['files'].append(attr.filename)
        elif on == 'local':
            for (top, dirs, files) in os.walk(top):
                if top not in tree:
                    tree[top] = {'dirs': [], 'files': []}

                if len(dirs) > 0:
                    tree[top]['dirs'].extend(dirs)

                if len(files) > 0:
                    tree[top]['files'].extend(files)

        return tree

    def upload(self, local_file_path: str, remote_file_path: str) -> None:
        self.sftp.put(local_file_path, remote_file_path)

    def get_download_missing_date(self, top: str, before_days: int) -> list:
        missing_info: list = []
        need_check_days: list = utils.get_ago_date_list(ago=before_days)

        for date in need_check_days:
            date_path = os.path.join(top, date)
            check_dir_exist: bool = self.check_exist(date_path, check_point='local')
            check_file_exist: bool = False

            try:
                check_file_exist = len(os.listdir(top)) > 0
            except WindowsError:
                self.logger.info("the directory of path doesn't contain files: {0}".format(date_path))

            if not check_dir_exist or not check_file_exist:
                missing_info.append(date)

        return missing_info

    def replace_path_format_local_to_remote(self, target_path: str) -> str:
        cleaned_path: str = ''

        if '\\' in target_path:
            cleaned_path = target_path.replace(self.local_default_dir_path, '').replace('\\', '/')
        elif '/' in target_path:
            cleaned_path = target_path.replace(self.local_default_dir_path, self.remote_default_dir_path)

        converted_path: str = self.remote_default_dir_path + cleaned_path

        return converted_path

    def _check_same_dir_tree(self, local_tree: dict, remote_tree: dict) -> bool:
        compare_tree: dict = {}

        for local_path, local_info in local_tree.items():
            converted_path: str = self.replace_path_format_local_to_remote(local_path)
            compare_tree[converted_path]: dict = local_info

        check_same: bool = (compare_tree == remote_tree)

        return check_same

    def get_upload_missing_info(self, local_tree: dict) -> dict:
        missing_info: dict = {}

        for local_path, local_info in local_tree.items():
            converted_path: str = self.replace_path_format_local_to_remote(local_path)

            for local_filename in local_info['files']:
                remote_file_path: str = utils.make_linux_path(converted_path, local_filename)

                if not self.check_exist(remote_file_path, check_point='remote'):
                    if local_path not in missing_info:
                        missing_info[local_path] = {'dirs': [], 'files': [local_filename]}
                    else:
                        missing_info[local_path]['files'].append(local_filename)

        return missing_info

    def close_session(self) -> None:
        self.sftp.close()
        self.transport.close()
        if self.ssh is not None:
            self.ssh.close()
        self.logger.info('Closed session')

