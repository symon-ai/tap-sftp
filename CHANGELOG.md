# Changelog

## 5.7.1
  * Sanitize user-supplied SFTP path/prefix and filename values before logging to remediate CWE-117 log forging (WP-33399)

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
