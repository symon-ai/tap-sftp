# Changelog

## 5.7.1
  * WP-33397: Sanitize the user-/config-controlled SFTP `prefix` path before it is written to the "Found no files" log warning in `client.py`, neutralizing carriage returns, line feeds, and other control characters to remediate a CWE-117 (log injection / log forging) finding.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
