import json

from pathlib import Path
from .aws import read_file, list_files


class Configurations:
    def __init__(self, dir, s3, bucket, prefix):
        self.loaded = self.load_specs(dir)
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix

    def load_specs(self, dir):
        return {file: {'json': self.read_configuration(file)} for file in dir.iterdir() if file.suffix == '.json'}

    def read_configuration(self, file):
        with open(file) as f:
            return json.load(f)

    def get(self):
        self.reload()
        return [self.loaded[key]['json'] for key in self.loaded.keys()]

    def load(self, file, last_modified):
        if file not in self.loaded or 'last_modified' not in self.loaded[file] or self.loaded[file][
            'last_modified'] < last_modified:
            self.loaded[file] = {'last_modified': last_modified,
                                 'json': json.load(read_file(self.s3, self.bucket, file))}

    def reload(self):
        if self.bucket is not "":
            for file, last_modified in list_files(self.s3, self.bucket, self.prefix):
                if file.endswith('.json'):
                    self.load(file, last_modified)