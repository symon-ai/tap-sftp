# Changelog

## 5.7.1
  * Require setuptools 83.x to fix CVE-2026-59890; align runtime and scanner locks.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
