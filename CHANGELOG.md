# Changelog

## 5.8.0
  * CWE-117 (WP-32465): Sanitize SFTP-server-supplied filenames/paths (strip CR/LF and other control characters) before writing them to log records to prevent log forging / log injection

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
