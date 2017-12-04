from gcp_census.config import Configuration


class View(object):
    def __init__(self, group, name, file_content):
        self.group = group
        self.name = name
        self.file_content = file_content

    def __str__(self):
        return "View(group={}, name={})".format(self.group, self.name)

    @property
    def query(self):
        return "".join(self.file_content)\
            .replace('$PROJECT_ID', Configuration.get_project_id())

    @property
    def description(self):
        desc_lines = [line[2:] for line in self.file_content
                      if line.startswith('--')]
        return "".join(desc_lines)
