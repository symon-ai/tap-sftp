# Changelog

## 5.7.1
  * Remove hard-coded credentials from SFTP test fixtures.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
