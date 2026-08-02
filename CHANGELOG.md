# Changelog

## 5.7.1
  * Security (CWE-117): sanitize CR/LF and other control characters in remote-supplied SFTP filenames/paths before writing them to logs, closing a log-forging / log-injection vector in `tap_sftp/client.py`.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
