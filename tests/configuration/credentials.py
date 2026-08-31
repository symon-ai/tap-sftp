import secrets


def ephemeral_sftp_credentials():
    return {
        "username": secrets.token_urlsafe(12),
        "password": secrets.token_urlsafe(24),
    }
