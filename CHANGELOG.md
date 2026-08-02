# Changelog

## 5.7.1
  * WP-33370 Remediate Veracode CWE-259: remove hard-coded passphrase literals from `tests/sftp_tests/test_helper.py` by deriving throwaway test passphrases at runtime

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
