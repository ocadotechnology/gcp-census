import unittest

from mock import patch

from gcp_census.config import Configuration
from gcp_census.model.view import View


class TestView(unittest.TestCase):

    @patch.object(Configuration, 'get_project_id', return_value='my-project')
    def test_should_parse_view(self, _):
        # given
        view = View('group', 'name',
                    ['--desc1\n', '--desc2\n', 'SELECT * FROM\n', 'table'])

        # when & then
        self.assertEqual(view.description, 'desc1\ndesc2\n')
        self.assertEqual(view.query, '--desc1\n--desc2\nSELECT * FROM\ntable')

    @patch.object(Configuration, 'get_project_id', return_value='my-project')
    def test_should_replace_project_id(self, _):
        # given
        view = View('group', 'name', ['--desc1\n', 'SELECT * FROM\n',
                                      '`$PROJECT_ID.dataset.table`'])

        # when & then
        self.assertEqual(view.description, 'desc1\n')
        self.assertEqual(view.query, '--desc1\nSELECT * FROM\n'
                                     '`my-project.dataset.table`')
