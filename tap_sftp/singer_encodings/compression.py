import gzip
import zipfile
import os


def infer(iterable, file_name):
    """Uses the incoming file_name and checks the end of the string
    for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith(tuple(['.tar.gz', '.zip.gpg', '.zip.pgp'])):
        root, ext = os.path.splitext(file_name)
        raise NotImplementedError(f"{os.path.splitext(root)[1]+ext} not supported")
    elif file_name.endswith('.gz'):
        yield gzip.GzipFile(fileobj=iterable)
    elif file_name.endswith('.zip'):
        with zipfile.ZipFile(iterable) as zip:
            for name in zip.namelist():
                if name.startswith('__MACOSX'):
                    continue
                yield zip.open(name)
    else:
        yield iterable
