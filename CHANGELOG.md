# Changelog

## 6.0.0
  * Upgrade Paramiko to 5.0.1 and setuptools to 83.0.0 for CVE-2026-44405 and CVE-2026-59890.
  * SFTP servers must support SHA-2 RSA signatures (or non-RSA host keys) and modern key exchange. DSA keys, RSA/SHA-1 signatures, SHA-1 key exchange and GSSAPI are no longer supported by Paramiko.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
