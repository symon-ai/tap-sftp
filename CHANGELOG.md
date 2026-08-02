# Changelog

## 5.7.1
  * WP-33406: Fix CWE-73 (path manipulation) in `client.py` by centralizing local destination path validation. Local paths derived from remote SFTP filenames are now sanitized and anchored inside the temp directory before being passed to `open()` / `sftp.get()` / `os.path.getsize()`.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
