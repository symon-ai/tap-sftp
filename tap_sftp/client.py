import logging
import os
import re
import stat
import tempfile
import time
from datetime import datetime
import backoff
import paramiko
import pytz
import singer
from paramiko.ssh_exception import AuthenticationException, SSHException
from tap_sftp import decrypt
from tap_sftp import sample_generator

LOGGER = singer.get_logger()
logging.getLogger("paramiko").setLevel(logging.CRITICAL)


def handle_backoff(details):
    LOGGER.warn(
        "SSH Connection closed unexpectedly. Waiting {wait} seconds and retrying...".format(**details)
    )


class SFTPConnection():
    def __init__(self, host, username, password=None, private_key_file=None, port=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port or 22)
        self.decrypted_file = None
        self.key = None
        self.transport = None
        self.retries = 10
        self.__sftp = None
        if private_key_file:
            key_path = os.path.expanduser(private_key_file)
            self.key = paramiko.RSAKey.from_private_key_file(key_path)

    # If connection is snapped during connect flow, retry up to a
    # minute for SSH connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(
        backoff.expo,
        (EOFError),
        max_tries=6,
        on_backoff=handle_backoff,
        jitter=None,
        factor=2)
    def __connect(self):
        for i in range(self.retries + 1):
            try:
                LOGGER.info('Creating new connection to SFTP...')
                self.transport = paramiko.Transport((self.host, self.port))
                self.transport.use_compression(True)
                self.transport.default_window_size = paramiko.common.MAX_WINDOW_SIZE
                self.transport.packetizer.REKEY_BYTES = pow(2, 40)
                self.transport.packetizer.REKEY_PACKETS = pow(2, 40)
                self.transport.connect(username=self.username, password=self.password, hostkey=None, pkey=self.key)
                self.__sftp = paramiko.SFTPClient.from_transport(self.transport)

                LOGGER.info('Connection successful')
                break
            except (AuthenticationException, SSHException) as ex:
                if self.__sftp:
                    self.__sftp.close()
                if self.transport:
                    self.transport.close()
                time.sleep(5 * i)
                LOGGER.info('Connection failed, retrying...')
                if i >= (self.retries):
                    raise ex

    @property
    def sftp(self):
        if self.__sftp is None:
            self.__connect()
        return self.__sftp

    @sftp.setter
    def sftp(self, sftp):
        self.__sftp = sftp

    def close(self):
        self.__sftp.close()
        self.transport.close()

    def match_files_for_table(self, files, table_name, search_pattern):
        LOGGER.info("Searching for files for table '%s', matching pattern: %s", table_name, search_pattern)
        matcher = re.compile(search_pattern)
        return [f for f in files if matcher.search(f["filepath"])]

    def is_empty(self, file_attr):
        return file_attr.st_size == 0

    def is_directory(self, file_attr):
        return stat.S_ISDIR(file_attr.st_mode)

    def get_files_by_prefix(self, prefix, search_subdirectories=True):
        """
        Accesses the underlying file system and gets all files that match "prefix", in this case, a directory path.

        Returns a list of filepaths from the root.
        """
        files = []

        if prefix is None or prefix == '':
            prefix = '.'

        try:
            result = self.sftp.listdir_attr(prefix)
        except FileNotFoundError as e:
            raise Exception("Directory '{}' does not exist".format(prefix)) from e

        for file_attr in result:
            # NB: This only looks at the immediate level beneath the prefix directory
            if self.is_directory(file_attr) and search_subdirectories:
                files += self.get_files_by_prefix(prefix + '/' + file_attr.filename)
            else:
                if self.is_empty(file_attr):
                    continue

                last_modified = file_attr.st_mtime
                if last_modified is None:
                    LOGGER.warning("Cannot read m_time for file %s, defaulting to current epoch time",
                                   os.path.join(prefix, file_attr.filename))
                    last_modified = datetime.utcnow().timestamp()

                # NB: SFTP specifies path characters to be '/'
                #     https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
                files.append({"filepath": prefix + '/' + file_attr.filename,
                              "last_modified": datetime.utcfromtimestamp(last_modified).replace(tzinfo=pytz.UTC),
                              "file_size": file_attr.st_size})

        return files

    def get_files(self, prefix, search_pattern, modified_since=None, search_subdirectories=True):
        files = self.get_files_by_prefix(prefix, search_subdirectories)
        if files:
            LOGGER.info('Found %s files in "%s"', len(files), prefix)
        else:
            LOGGER.warning('Found no files on specified SFTP server at "%s"', prefix)

        matching_files = self.get_files_matching_pattern(files, f'{search_pattern}$')

        if matching_files:
            LOGGER.info('Found %s files in "%s" matching "%s"', len(matching_files), prefix, search_pattern)
        else:
            LOGGER.warning('Found no files on specified SFTP server at "%s" matching "%s"', prefix, search_pattern)

        for f in matching_files:
            LOGGER.info("Found file: %s", f['filepath'])

        if modified_since is not None:
            matching_files = [f for f in matching_files if f["last_modified"] > modified_since]

        return matching_files

    def get_file_handle(self, f, decryption_configs=None):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and returns a handle to the file. """
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            sftp_file_path = f["filepath"]
            local_path = f'{tmp_dir_name}/{os.path.basename(sftp_file_path)}'
            if decryption_configs:
                decrypt_remote = decryption_configs.get("decrypt_remote", True)
                LOGGER.info(f'Decrypting file: {sftp_file_path}')
                if not decrypt_remote:
                    self.sftp.get(sftp_file_path, local_path)
                    decrypted_path = decrypt.gpg_decrypt(local_path, tmp_dir_name, decryption_configs.get('key'),
                                                         decryption_configs.get('gnupghome'),
                                                         decryption_configs.get('passphrase'))
                else:
                    with self.sftp.open(sftp_file_path, 'rb', 32768) as sftp_file:
                        sftp_file.prefetch()
                        decrypted_path = decrypt.gpg_decrypt_from_remote(sftp_file, sftp_file_path, tmp_dir_name,
                                                                         decryption_configs.get('key'),
                                                                         decryption_configs.get('gnupghome'),
                                                                         decryption_configs.get('passphrase'))
                try:
                    return open(decrypted_path, 'rb')
                except FileNotFoundError:
                    raise Exception(f'Decryption of file failed: {sftp_file_path}')
            else:
                self.sftp.get(sftp_file_path, local_path)
                return open(local_path, 'rb')

    def get_file_handle_for_sample(self, f, decryption_configs=None, max_records=None):
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            sftp_file_path = f["filepath"]
            sftp_file_name = os.path.basename(sftp_file_path)
            with self.sftp.open(sftp_file_path, "rb") as sftp_file_object:
                if decryption_configs:
                    sample_file = sample_generator.generate_sample_for_encrypted(sftp_file_object, sftp_file_path, decryption_configs, tmp_dir_name, max_records)
                    try:
                        return open(sample_file, 'rb')
                    except FileNotFoundError:
                        raise Exception(f'Decryption of file failed: {sftp_file_path}')
                else:
                    sample_file = sample_generator.generate_sample(sftp_file_object, sftp_file_name, tmp_dir_name, max_records)
                    return open(sample_file, "rb")

    def get_files_matching_pattern(self, files, pattern):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and a regex pattern string, and returns
            files matching that pattern. """
        matcher = re.compile(pattern)
        LOGGER.info(f"Searching for files for matching pattern: {pattern}")
        return [f for f in files if matcher.search(f["filepath"])]


def connection(config):
    return SFTPConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          private_key_file=config.get('private_key_file'),
                          port=config.get('port'))
