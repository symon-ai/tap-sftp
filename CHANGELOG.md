# Changelog

## 5.10.0
  * WP-36206: Set security-overrides floors `fsspec` >= 2026.6.0 (CVE-2026-104851) and `multidict` >= 6.9.1 (CVE-2026-104874); locks resolve to those patched versions. Bump `file-processors` to v4.10.0.
  * The app-api import-activity consumer must select connector tag v5.10.0 after release. The scanner mirror remains flattened; the application installs without the optional dev group.

## 5.9.0
  * WP-36464: Pin optional development dependencies to filelock 4.2.1 (CVE-2026-112940, CVE-2026-112907, CVE-2026-112913) and virtualenv 21.14.9 (CVE-2026-113721, CVE-2026-113719). Refresh both dependency locks, including required packaging and python-discovery updates.
  * The app-api import-activity consumer must select connector tag v5.9.0 after release. The scanner mirror remains flattened; the application installs without the optional dev group.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
