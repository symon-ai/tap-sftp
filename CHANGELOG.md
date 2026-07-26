# Changelog

## 5.7.1
  * Bump setuptools to 83.0.0 to remediate CVE-2026-59890 (WP-32461).

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
