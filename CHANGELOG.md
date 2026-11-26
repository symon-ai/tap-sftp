# Changelog

## 5.7.1
  * Upgrade locked setuptools to 83.0.0 for CVE-2026-59890.
  * CVE-2026-44405 remains unresolved: file-processors v4.7.1 requires paramiko 3.4.0.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
