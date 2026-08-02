# Changelog

## 5.7.1
  * WP-33404: Sanitize CR/LF and control characters in remote SFTP file paths before logging to remediate a CWE-117 log-forging finding in `tap_sftp/client.py`

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
