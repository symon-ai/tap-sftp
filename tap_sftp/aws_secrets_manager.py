import boto3


class AWSSecretsManager:

    _client = None

    def __init__(self, region):
        session = boto3.session.Session()
        self._client = session.client(
            service_name='secretsmanager',
            region_name=region
        )

    def get_secret(self, secret_id):
        kwargs = {'SecretId': secret_id}
        response = self._client.get_secret_value(**kwargs)
        secure_string = response['SecretString']
        return secure_string
