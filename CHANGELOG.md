# Changelog

## 5.7.1
  * WP-33413: Remediate CWE-73 path manipulation in `client.py` by validating that locally-constructed download/decrypt paths stay within the temporary download directory before they reach `os.path.getsize()`/`open()`.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
