# Changelog

## 5.7.1
  * Remediate CWE-117 (log forging): sanitize user-controlled SFTP file paths/names before writing them to logs (strip CR/LF and other control characters)

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
