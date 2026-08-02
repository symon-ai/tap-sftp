# Changelog

## 5.7.1
  * WP-33387: Validate the user-supplied `private_key_file` path before use to remediate a CWE-73 (external control of file name / path) finding. The path is now confined to an allowed key directory and traversal / null-byte input is rejected with a `SymonException`.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
