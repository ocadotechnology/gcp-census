import json
import os

from gcp_census.model.table import Table
from gcp_census.model.view import View


class FilesystemModelProvider(object):

    def __init__(self, model_directory):
        self.model_directory = model_directory

    def list_tables(self):
        for table in self.__list_files('.json'):
            with open(table[2]) as json_file:
                json_dict = json.load(json_file)
                yield Table(table[0], table[1], json_dict)

    def list_views(self):
        for view in self.__list_files('.sql'):
            with open(view[2]) as view_file:
                content = view_file.readlines()
                yield View(view[0], view[1], content)

    def list_groups(self):
        for group_dir in os.listdir(self.model_directory):
            subdirectory = os.path.join(self.model_directory, group_dir)
            if os.path.isdir(subdirectory):
                yield group_dir

    def __list_files(self, extension):
        for group_dir in os.listdir(self.model_directory):
            subdirectory = os.path.join(self.model_directory, group_dir)
            if os.path.isdir(subdirectory):
                for model_file in os.listdir(subdirectory):
                    if model_file.endswith(extension):
                        model_name = os.path.splitext(model_file)[0]
                        filename = os.path.join(self.model_directory, group_dir,
                                                model_file)
                        yield group_dir, model_name, filename
