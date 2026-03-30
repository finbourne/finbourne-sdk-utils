import pytest

import numpy as np
import pandas as pd

import finbourne_sdk_utils.cocoon as cocoon
from finbourne_sdk_utils import logger
from  .mock_api_factory import MockApiFactory


class TestCocoonInvalidProperties:
    @classmethod
    def setup_class(cls) -> None:
        cls.api_factory = MockApiFactory( )
        cls.logger = logger.LusidLogger("debug")

    @pytest.mark.parametrize(
        "data_frame, expected_message",
        [
            (
                pd.DataFrame([{"a": 2}]).astype(np.short),
                "The following columns in the data_frame have not been mapped to LUSID data types: {'a': 'int16'}",
            ),
        ],
    )
    def test_create_property_definitions_from_file(
        self, data_frame, expected_message
    ) -> None:
        with pytest.raises(TypeError) as exc_info:
            cocoon.properties.create_property_definitions_from_file(
                api_factory=self.api_factory,
                column_to_scope={column: "abc" for column in data_frame.columns.to_list()},
                domain="def",
                data_frame=data_frame,
                missing_property_columns=data_frame.columns.to_list(),
            )
        assert expected_message in str(exc_info.value), str(exc_info.value)

    @pytest.mark.parametrize(
        "row, dtypes, expected_message",
        [
            (
                pd.Series(data=[1], index=["a"]),
                pd.Series(data=["int16"], index=["a"]),
                "The following columns in the data_frame have not been mapped to LUSID data types: {'a': 'int16'}",
            ),
        ],
    )
    def test_create_property_values(self, row, dtypes, expected_message) -> None:
        with pytest.raises(TypeError) as exc_info:
            cocoon.properties.create_property_values(
                row=row, scope="abc", domain="def", dtypes=dtypes, column_to_scope={column: "abc" for column in row}
            )
        assert expected_message in str(exc_info.value), str(exc_info.value)
