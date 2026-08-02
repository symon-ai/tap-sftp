# Changelog

## 5.7.1
  * WP-33373: Remediate CWE-73 (path manipulation) — validate the user-supplied `error_file_path` config via a centralized `sanitize_error_file_path` helper that constrains it to the tap working directory before it is passed to `open()`, rejecting absolute-path and `..` traversal escapes.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
