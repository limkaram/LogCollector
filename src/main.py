from src.package import utils, handler, server
import os
import traceback
import time

params: dict = utils.file_to_dict(os.path.join('..', 'conf', 'config.yaml'))
MISSING_DATE_CHECK_PERIOD: int = params['MISSING_DATE_CHECK_PERIOD']
LOG_CONFIG_FILE_PATH: str = os.path.join('..', 'conf', 'logger.yaml')
PRODUCTION_CONFIG_FILE_PATH: str = os.path.join('..', 'conf', 'production.yaml')
UPLOAD_MISSING_DATES = []


class Main:
    def __init__(self):
        try:
            self.logger = utils.get_logger_config(LOG_CONFIG_FILE_PATH, logger_name='main')
            productions_info: dict = utils.file_to_dict(PRODUCTION_CONFIG_FILE_PATH)

            for info in productions_info['PRODUCTION_INFO']:
                server_ = server.SFTPServer(info)
                controller = handler.SFTPHandler()
                self.logger.info(
                    f'Attempting to create an SFTP session(host:{server_.host}, username:{server_.username})')
                controller.create_sftp_session(server_.host, server_.username, server_.password, server_.port)
                self.logger.info(
                    f'SFTP session created successfully(host:{server_.host}, username:{server_.username})')

                if server_.purpose == 'download':
                    controller.set_default_path(server_.local_default_dir_path)
                    missing_dates = controller.get_download_missing_date(
                        os.path.join(server_.local_default_dir_path, server_.production_name),
                        MISSING_DATE_CHECK_PERIOD)

                    if len(missing_dates) == 0:
                        self.logger.info('All files have been downloaded completely')
                        self.logger.info(f'Pass this server({server_.host}, {server_.production_name})')
                        continue
                    elif len(missing_dates) > 0:
                        for missing_date in missing_dates:
                            for file_info in server_.download_files_info:
                                self.logger.info(f'Attempting to download file, {file_info}')
                                filename: str = utils.change_filename_date(file_info['FILENAME_FORMAT'], missing_date)
                                remote_dir_path: str = file_info['DIR_PATH']
                                remote_file_path: str = utils.make_linux_path(remote_dir_path, filename)

                                if not controller.check_exist(path=remote_file_path, check_point='remote'):
                                    self.logger.info(f'remote file is not exist in {remote_file_path}')
                                    self.logger.info(f'Pass the file download process, {remote_file_path}')
                                    continue
                                else:
                                    production_name_dir_path: str = os.path.join(server_.local_default_dir_path, server_.production_name)
                                    date_dir_path: str = os.path.join(production_name_dir_path, missing_date)

                                    if not controller.check_exist(production_name_dir_path, check_point='local'):
                                        self.logger.info(f'not exist, try to make directory({production_name_dir_path})')
                                        controller.mkdir(production_name_dir_path, to='local')

                                    if not controller.check_exist(date_dir_path, check_point='local'):
                                        self.logger.info(f'not exist, try to make directory({date_dir_path})')
                                        controller.mkdir(date_dir_path, to='local')

                                    local_file_path = os.path.join(date_dir_path, filename)
                                    self.logger.info(f'Downloading..., [{remote_file_path}] to [{local_file_path}]')
                                    download_start_time = time.time()
                                    try:
                                        controller.download(remote_file_path, local_file_path)
                                    except Exception as e:
                                        self.logger.error(f'{e} :: check the permission {remote_file_path}')
                                        self.logger.info(f'pass to download this file({remote_file_path})')
                                        continue
                                    download_end_time = time.time()

                                    if controller.check_file_transfer_integrity(local_file_path, remote_file_path,
                                                                                purpose='download'):
                                        elapsed_time = download_end_time - download_start_time
                                        self.logger.info(f'Download complete! elapsed time : {elapsed_time:.2f}s')
                                    else:
                                        self.logger.error('local file is not exist or local and remote filesize is not same')
                                        continue
                elif server_.purpose == 'upload':
                    controller.set_default_path(server_.local_default_dir_path, server_.remote_default_dir_path)
                    local_tree = controller.walk_dir(server_.local_default_dir_path, on='local')
                    missing_info = controller.get_upload_missing_info(local_tree)
                    missing_dates = [os.path.basename(i) for i in missing_info.keys()]
                    UPLOAD_MISSING_DATES.extend(missing_dates)

                    if len(missing_info) == 0:
                        self.logger.info('There is no missing file')
                        self.logger.info(f'Pass this server({server_.host})')
                        continue
                    elif len(missing_info) > 0:
                        local_date_dir_paths = missing_info.keys()

                        for local_date_dir_path in local_date_dir_paths:
                            self.logger.info(f'current local_date_dir_path : {local_date_dir_path}')
                            necessary_dirs = local_date_dir_path.replace(server_.local_default_dir_path, '').strip('\\').split('\\')
                            remote_dir_path = server_.remote_default_dir_path

                            for element in necessary_dirs:
                                remote_dir_path = utils.make_linux_path(remote_dir_path, element)

                                if not controller.check_exist(remote_dir_path, check_point='remote'):
                                    self.logger.info(f'not exist, try to make directory({remote_dir_path})')
                                    controller.mkdir(remote_dir_path, to='remote')

                            files = missing_info[local_date_dir_path]['files']

                            for file in files:
                                local_file_path = os.path.join(local_date_dir_path, file)
                                remote_file_path = controller.replace_path_format_local_to_remote(local_file_path)
                                upload_start_time = time.time()
                                controller.upload(local_file_path, remote_file_path)
                                upload_end_time = time.time()

                                if controller.check_file_transfer_integrity(local_file_path, remote_file_path, purpose='upload'):
                                    elapsed_time = upload_end_time - upload_start_time
                                    self.logger.info(f'Upload complete! elapsed time : {elapsed_time:.2f} :: [{local_file_path}] to [{remote_file_path}]')
                                else:
                                    self.logger.error('local file is not exist or local and remote filesize is not same')
                                    continue
                elif server_.purpose == 'upload_to_hdfs':
                    missing_dates = utils.remove_duplicates(UPLOAD_MISSING_DATES)
                    command = server_.shell_command.replace('{REMOTE_PATH}', server_.remote_default_dir_path).replace('{MISSING_DATE_STRING}', utils.list2str(missing_dates))
                    self.logger.info(
                        f'Attempting to create an SSH session(host:{server_.host}, username:{server_.username})')
                    controller.create_ssh_session(server_.host, server_.username, server_.password, server_.port)
                    self.logger.info(
                        f'SFTP session created successfully(host:{server_.host}, username:{server_.username})')
                    controller.create_command_channel()
                    self.logger.info('command_channel created successfully')
                    controller.send_shell_command(command)
                    self.logger.info(f'send command :: {command}')
                controller.close_session()
        except Exception as e:
            error_line, error_method = utils.get_error_location(traceback.format_exc(limit=1))
            self.logger.error(f'line {error_line}, {error_method} :: {e}')


if __name__ == '__main__':
    main = Main()
