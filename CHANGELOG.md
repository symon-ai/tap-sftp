# Changelog

## 5.7.1
  * Remediate CWE-117 (log forging): sanitize SFTP-supplied file paths, filenames, prefixes, and search patterns before writing them to logs.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
