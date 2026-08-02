# Changelog

## 5.8.0
  * WP-33428: Remediate Veracode CWE-73 (external control of file name / path) in `tap_sftp/client.py`. Locally-constructed sample/decrypt paths are now validated with a centralized `helper.validate_path_in_directory` routine that resolves the candidate path and confines it to the intended temporary directory before it is passed to `open()`, preventing crafted filenames from escaping the temp dir.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
