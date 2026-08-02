# Changelog

## 5.7.1
  * Remediate CWE-117 (log forging) in `tap_sftp/client.py` by sanitizing CR/LF and other control characters out of user-supplied values (SFTP directory prefix, filename search pattern, and derived filepaths) before they are written to log entries

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
