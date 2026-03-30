import os
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.instruments import prepare_key
import pytest


class TestCocoonInstruments:
    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "identifier_lusid, full_key_format, expected_outcome",
        [
            ("Instrument/default/Figi", True, "Instrument/default/Figi"),
            ("Figi", True, "Instrument/default/Figi"),
            ("Instrument/PB/Isin", True, "Instrument/PB/Isin"),
            ("Isin", False, "Isin"),
            ("Instrument/default/Isin", False, "Isin"),
        ],
    )
    def test_create_identifiers_prepare_key(
        self, identifier_lusid, full_key_format, expected_outcome
    ) -> None:
        """
        Tests that key preparation for identifiers works as expected

        :param _: The name of the test
        :param str identifier_lusid: The LUSID identifier
        :param bool full_key_format: The full key format
        :param str expected_outcome: The expected output key

        :return: None
        """

        output_key = prepare_key(
            identifier_lusid=identifier_lusid, full_key_format=full_key_format
        )

        assert output_key == expected_outcome
