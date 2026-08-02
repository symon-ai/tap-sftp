# Changelog

## 5.7.1
  * WP-33419: Sanitize tainted SFTP path/filename values (strip CR/LF and other control characters) before logging to remediate CWE-117 log forging in `tap_sftp/client.py`

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
