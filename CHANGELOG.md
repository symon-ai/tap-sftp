# Changelog

## 5.7.1
  * WP-33430: Sanitize the user-supplied search `pattern` before logging in `get_files_matching_pattern` to prevent CWE-117 log forging (CR/LF/control-character injection)

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
