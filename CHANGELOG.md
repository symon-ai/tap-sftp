# Changelog

## 5.8.0
  * Upgrade locked setuptools to 83.0.0 for CVE-2026-59890 and synchronize the scanner dependency snapshots.
  * Paramiko CVE-2026-44405 remains unresolved: no patched version was supplied by the advisory.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
