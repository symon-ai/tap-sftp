# Changelog

## 5.7.1
  * WP-32464: Remediate Veracode CWE-80 in `tap_sftp/helper.py` `sample_file()` — write each zip archive entry with a sanitized base-name `arcname` (`os.path.basename(path)`) instead of the raw, source-supplied on-disk `path`, so untrusted input can no longer control the archive entry name and the local directory structure is no longer leaked into the archive.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
