# Changelog

## Unreleased
  * Require setuptools >=84.1.0 to fix CVE-2026-59890 without introducing the known wheel-installation vulnerability in 83.0.0.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
