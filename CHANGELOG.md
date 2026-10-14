# Changelog

## 5.7.1
  * Pin production setuptools to 83.0.0 for CVE-2026-59890 and refresh the scanner inventory.
  * Paramiko remains at 3.4.0: CVE-2026-44405 has no advisory-published patched release.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
