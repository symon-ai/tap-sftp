# Changelog

## 5.7.1
  * WP-32463 (CWE-73): contain the user-supplied `error_file_path` within an allowed base directory before opening it, rejecting path-traversal input

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
