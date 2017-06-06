import unittest
from mock import patch

from gcp_census.decorators import retry


class TestRetryDecorator(unittest.TestCase):

    class TestClass(object):
        def counter_for_FPE(self):
            pass

        def counter_for_AE(self):
            pass

        @retry((FloatingPointError, AttributeError), tries=3, delay=0, backoff=1)# nopep8 pylint: disable=C0301
        def raise_FloatingPointError(self):
            self.counter_for_FPE()
            raise FloatingPointError

        @retry((FloatingPointError, AttributeError), tries=3, delay=0, backoff=1)# nopep8 pylint: disable=C0301
        def raise_AttributeError(self):
            self.counter_for_AE()
            raise AttributeError

    @patch.object(TestClass, "counter_for_FPE")
    @patch.object(TestClass, "counter_for_AE")
    def test_that_multiple_errors_in_retry_works(
            self,
            counter_for_AE,
            counter_for_FPE
    ):
        test_class = TestRetryDecorator.TestClass()
        #when
        self.execute_function_and_suppress_exceptions(
            test_class.raise_AttributeError)
        self.execute_function_and_suppress_exceptions(
            test_class.raise_FloatingPointError)
        #then
        self.assertEquals(3, counter_for_FPE.call_count)
        self.assertEquals(3, counter_for_AE.call_count)

    @classmethod
    def execute_function_and_suppress_exceptions(cls, function):
        try:
            function()
        except Exception:
            pass
