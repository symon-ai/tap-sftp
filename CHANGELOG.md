# Changelog

## 5.7.1
  * Fix CWE-117 (log injection / log forging): sanitize CR/LF and other control characters in user-supplied SFTP paths, prefixes, filenames, and search patterns before they are written to log records (`tap_sftp/client.py`).

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
