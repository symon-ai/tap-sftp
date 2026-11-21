# Changelog

## Unreleased
  * Require setuptools 83.x to address CVE-2026-59890 in the runtime and scan dependency locks.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
