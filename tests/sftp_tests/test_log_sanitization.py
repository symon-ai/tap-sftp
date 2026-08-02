import logging
from unittest.mock import patch

from tap_sftp.client import SFTPConnection, _sanitize_for_log


def test_sanitize_for_log_strips_crlf_and_control_chars():
    """WP-33416: CR/LF and other C0 control characters in tainted values must be
    neutralized so they cannot forge new log entries (CWE-117)."""
    forged = "orders.csv\r\nINFO Injected forged log line\x00\x1b[31m"
    sanitized = _sanitize_for_log(forged)
    assert "\r" not in sanitized
    assert "\n" not in sanitized
    assert "\x00" not in sanitized
    assert "\x1b" not in sanitized
    # The visible filename content is preserved; only control chars are replaced.
    assert "orders.csv" in sanitized
    assert "Injected forged log line" in sanitized


def test_sanitize_for_log_passes_through_none():
    assert _sanitize_for_log(None) is None


@patch('tap_sftp.client.find_encoding.find_encoding_v2', return_value='utf-8')
def test_get_text_encoding_neutralizes_tainted_local_path_in_logs(mock_find_encoding, caplog):
    """WP-33416: a local_path carrying CRLF-injected forged content must not
    produce multi-line / control-character log records when logged at line ~248."""
    tainted_path = "/tmp/orders.csv\r\nCRITICAL Forged administrator login succeeded"
    with caplog.at_level(logging.INFO, logger='singer'):
        SFTPConnection._get_text_encoding(tainted_path, None, file_size=1024)

    encoding_records = [r for r in caplog.records
                        if 'Detecting SFTP text file encoding' in r.getMessage()]
    assert encoding_records, "expected the encoding-detection log record to be emitted"
    for record in encoding_records:
        message = record.getMessage()
        assert "\r" not in message
        assert "\n" not in message
