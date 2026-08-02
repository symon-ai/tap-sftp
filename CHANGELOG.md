# Changelog

## 5.7.1
  * WP-33424 Remediate CWE-73 path manipulation in `tap_sftp/client.py`: route the local download path through a centralized `_safe_local_path` validation routine that reduces the remote filename to a basename and verifies the resolved path stays inside the temp working directory before it reaches `os.path.getsize` / `open` / `sftp.get`.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
