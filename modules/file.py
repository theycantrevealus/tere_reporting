import os

class File:
    # def __init__(self):

    def backup(self, path, zip_name):
        os.system(f'zip -r {zip_name} {path}')