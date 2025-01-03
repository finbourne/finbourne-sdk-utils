import os
import unittest
import finbourne_sdk_utils.cocoon as cocoon
from parameterized import parameterized
from finbourne_sdk_utils import logger


class CocoonUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @parameterized.expand(
        [
            ["Standard Base URL", "https://fbn-prd.lusid.com/api"],
            ["Base URL with forward slash suffix", "https://fbn-prd.lusid.com/api/"],
        ]
    )
    def test_get_swagger_dict_success(self, _, api_url):

        swagger_dict = cocoon.utilities.get_swagger_dict(api_url=api_url)

        self.assertTrue(expr=isinstance(swagger_dict, dict))

    def test_get_swagger_dict_fail(self):

        with self.assertRaises(ValueError):
            cocoon.utilities.get_swagger_dict(api_url="https://fbn-prd.lusid.com")
