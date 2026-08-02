# Changelog

## 5.7.1
  * WP-33429: Remediate CWE-73 path manipulation in `get_file_handle_for_sample` by validating the derived sample/decrypt path stays within the intended temporary directory before `open()`

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
