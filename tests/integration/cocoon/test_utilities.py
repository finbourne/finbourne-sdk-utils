import os
import pytest
import finbourne_sdk_utils.cocoon as cocoon
from finbourne_sdk_utils import logger


class TestCocoonUtilities:
    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "api_url",
        [
            "https://fbn-prd.lusid.com/api",
            "https://fbn-prd.lusid.com/api/",
        ],
    )
    def test_get_swagger_dict_success(self, api_url):

        swagger_dict = cocoon.utilities.get_swagger_dict(api_url=api_url)

        assert isinstance(swagger_dict, dict)

    def test_get_swagger_dict_fail(self):

        with pytest.raises(ValueError):
            cocoon.utilities.get_swagger_dict(api_url="https://fbn-prd.lusid.com")
