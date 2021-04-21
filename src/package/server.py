class SFTPServer:
    def __init__(self, production_info: dict):
        self.purpose: str = production_info['PURPOSE']
        self.host: str = production_info['HOST']
        self.port: int = production_info['PORT']
        self.username: str = production_info['USERNAME']
        self.password: str = production_info['PASSWORD']
        self.local_default_dir_path: str = production_info['LOCAL_PATH']

        if self.purpose == 'download':
            self.production_name: str = production_info['NAME']
            self.files_info: list = production_info['FILES_INFO']
        elif self.purpose == 'upload':
            self.remote_default_dir_path: str = production_info['REMOTE_PATH']
        elif self.purpose == 'upload_to_hdfs':
            self.shell_command: str = production_info['SHELL_COMMAND']
            self.remote_default_dir_path: str = production_info['REMOTE_PATH']

    @property
    def download_files_info(self) -> dict:
        for info in self.files_info:
            yield info

