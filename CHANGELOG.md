# Changelog

## 5.8.0
  * WP-33421: Sanitize CR/LF and other control characters from user-supplied
    values (remote SFTP file/directory names, prefixes, search patterns) before
    they are written to logs in `tap_sftp/client.py`, remediating CWE-117
    (Improper Output Neutralization for Logs / log forging). Only the logged
    representation is neutralized; the file paths used for SFTP/filesystem
    operations are unchanged.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
