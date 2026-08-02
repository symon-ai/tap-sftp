# Changelog

## 5.7.2
  * Remediate CWE-73 path manipulation in `helper.sample_file`: sanitize the user/remote-supplied SFTP file name (strip directory components) and verify the resolved output path is contained within `out_dir` before writing/zipping, raising `SymonException` for degenerate names.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
