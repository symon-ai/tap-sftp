class Capturer(object):
    def __init__(self, out_file_path, max_records=None):
        self._max_records = max_records
        self.out_file_path = out_file_path
        self._data = b''
        self._total_lines = 0
        self.out_file = open(out_file_path, "wb")

    def __call__(self, data):
        if data:
            if not self._max_records:
                self.out_file.write(data)
            else:
                self._data += data
                lines = len(self._data.split(b'\n'))
                if lines > self._max_records:
                    self.out_file.write(b'\n'.join(data.split(b'\n')[0:lines - self._max_records]))
                    self.out_file.close()
                    return False, True
                else:
                    self.out_file.write(data)
        else:
            self.out_file.close()

        return False, False
