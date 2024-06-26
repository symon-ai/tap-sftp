# tap-sftp
![Tests](https://github.com/symon-ai/tap-sftp/actions/workflows/tests.yml/badge.svg)

[Singer](https://www.singer.io/) tap that extracts data from SFTP files and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

### Install and Run

Ensure poetry is installed on your machine. 

- This command will return the installed version of poetry if it is installed.
```
poetry --version
```

- If not, install poetry using the following commands (from https://python-poetry.org/docs/#installation):
```
curl -sSL https://install.python-poetry.org | python3 -
PATH=~/.local/bin:$PATH
```

Within the `tap-sftp` directory, install dependencies:
```
poetry install
```

Then run the tap:
```
poetry run tap-sftp <options>
```

## Configuration:

1. Create a `config.json` file with connection details to snowflake.

   ```json
   {
        "host": "SFTP_HOST_NAME",
        "port": 22,
        "username": "YOUR_USER",
        "password": "YOUR_PASS",
        "search_subdirectories": false,
        "show_stats": true,
        "tables": [
            {
                "table_name": "Orders",
                "search_prefix": "\/Orders\/SubFolder",
                "search_pattern": "Orders.*\\.csv",
                "key_properties": [],
                "file_type": "csv",
                "delimiter": ",",
                "quotechar": "\"",
                "encoding": "utf-8"
            },
            {
                "search_prefix": "/Data",
                "search_pattern": "SampleFile.xlsx.gpg",
                "key_properties": [],
                "file_type": "excel",   
                "worksheets": ["SalesOrders", "SalesPolicy"],
                "has_header": true
            }    
        ],
        "start_date":"1900-01-01",
        "decryption_configs": {
            "SSM_key_name": "SSM_PARAMETER_KEY_NAME",
            "gnupghome": "/your/dir/.gnupg",
            "passphrase": "your_gpg_passphrase",
            "decrypt_remote": true
        },
        "private_key_file": "Optional_Path"
    }
   ```
   - **host**: URI of the SFTP server.
   - **port**: The port number of the SFTP service listening on server. Default is 22.
   - **username**: The username to connect to the server.
   - **password**: The password to authenticate. Leave this blank if private key file is used.
   - **search_subdirectories**: Flag indicates whether to search within the subdirectories or not. Set it to false if the path(defined in prefix) for the target file is known and subdirectory search is not required.
   - **show_stats**: Flag to show/hide sync stats. Default is false.
   - **max_file_size**: Maximum file size allowed. Default is 5242880 KB (5GB). Discovery will generate stream with empty properties and Sync will raise exception if file size is bigger than this.
   - **tables**: List of configurations which will be used to search files within the file hierarchy and read the target tables.
   - **table_name**: Name of the table should appear in the data stream for csv/text files. Not used in excel file.
   - **search_prefix**: Hierarchical path of the file(s) to be read.
   - **search_pattern**: Pattern to be used to search the file(s).
   - **key_properties**: Define mandatory column headers within the file.
   - **file_type**: Type of the file. Currently, supported types are csv, text and excel. 
   - **delimiter**: Delimiter used as separator in csv file.
   - **quotechar**: Specifies the character used to surround fields that contain the delimiter character. The default is a double quote ( ' " ' ).
   - **worksheets**: List of worksheets to be discovered/synced for an Excel file. Default is all worksheets.
   - **has_header**: Flag to indicate whether target file has header or not. Default is true.
   - **start_date**: Date since file(s) modified. 
   - **decryption_configs**: List of configurations that are used to decrypt encrypted file.
   - **key_storage_type**: Type of the key storage. Currently, supported "AWS_SSM" and "AWS_Secrets_Manager". Default is "AWS_Secrets_Manager". Saving "AWS_Secrets_Manager" requires storing key and passphrase as follows
     ```json
      {
     "privateKeyEncoded": {private key base64 encoded},
     "passphrase": passphrase as text
     }
     ```
   - **key_name**: Name of the key in storage location where the decryption private key/passphrase is stored. 
   - **gnupghome**: The home directory for gnupg. If folder doesn't exist, tap will try to create a folder. If not provided, a folder named gnupg will be created inside the current working directory. 
   - **passphrase**: Passphrase to decrypt encrypted file.
   - **decrypt_remote**: Flag indicates whether to decrypt from remote source directly. Default is true. 
   - **private_key_file**(optional): Provide path for private_key_file if private key will be used instead of password.

## Discovery mode:

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-sftp --config config.json --discover > catalog.json
```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

Edit the `catalog.json` and select the streams to replicate. Or use this helpful [discovery utility](https://github.com/chrisgoddard/singer-discover).

## Run Tap:

Run the tap like any other singer compatible tap:

```
$ tap-sftp --config config.json --catalog catalog.json
```

## To run tests:

1. Install python dependencies in a virtual env and run unit and integration tests
```
  $ python3 -m venv ~/.virtualenvs/tap-sftp
  $ source ~/.virtualenvs/tap-sftp/bin/activate
  $ pip install -U pip setuptools
  $ pip install --upgrade pip
  $ pip install -e .
  $ pip install tox
```

2. To run unit tests:
```
  tox -rp
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
