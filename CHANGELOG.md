# Changelog

## 5.7.1
  * Require setuptools 83.0.0 or newer to address CVE-2026-59890; synchronize the Poetry and Veracode dependency locks.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
