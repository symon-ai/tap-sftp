# Changelog

## 5.7.1
  * WP-33413: Remediate CWE-73 path manipulation in `client.py` by validating that locally-constructed download/decrypt paths stay within the temporary download directory before they reach `os.path.getsize()`/`open()`. Applied the same containment check to the sample flow (`get_file_handle_for_sample`) so traversal-crafted SFTP filenames are rejected there as well.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
