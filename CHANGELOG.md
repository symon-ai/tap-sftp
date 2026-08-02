# Changelog

## 5.7.1
  * Remediate CWE-73 (external control of file name/path) in `tap_sftp/helper.py`: confine files written by `sample_file` to `out_dir` by containing untrusted SFTP file names and archive member names to a safe basename within the output directory (WP-33376)

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
