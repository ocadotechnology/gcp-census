import unittest

from gcp_census.model.view import View


class TestModelCreator(unittest.TestCase):
    def test_should_parse_view(self):
        # given
        view = View('group', 'name',
                    ['--desc1\n', '--desc2\n', 'SELECT * FROM\n', 'table'])

        # when & then
        self.assertEqual(view.description, 'desc1\ndesc2\n')
        self.assertEqual(view.query, '--desc1\n--desc2\nSELECT * FROM\ntable')
