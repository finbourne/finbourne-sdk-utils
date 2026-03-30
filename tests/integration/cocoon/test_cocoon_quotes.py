import os
from pathlib import Path
import numpy as np
import pandas as pd

from finbourne_sdk_utils import cocoon as cocoon
import pytest
from finbourne.sdk.extensions import SyncApiClientFactory
from finbourne_sdk_utils import logger


class TestCocoonQuotes:
    @classmethod
    def setup_class(cls) -> None:

        cls.api_factory = SyncApiClientFactory()

        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "scope, file_name, quotes_mapping_required, quotes_mapping_optional, expected_outcome",
        [
            (
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"default": "2019-10-28"},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.var_field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": 30,
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
            ),
            (
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"column": "Valuation Date"},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.var_field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": 30,
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
            ),
            (
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"default": pd.Timestamp("2019-10-28")},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.var_field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": 30,
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
            ),
        ],
    )
    def test_load_from_data_frame_quotes_success(
        self,
        scope,
        file_name,
        quotes_mapping_required,
        quotes_mapping_optional,
        expected_outcome,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=quotes_mapping_required,
            mapping_optional=quotes_mapping_optional,
            file_type="quotes",
        )

        assert sum(
            [len(response.values) for response in responses["quotes"]["success"]]
        ) == len(data_frame)

        # Assert that by no unmatched_identifiers are returned in the response for quotes
        assert not responses["quotes"].get("unmatched_identifiers", False)

        assert sum(
            [len(response.failed) for response in responses["quotes"]["success"]]
        ) == 0

    @pytest.mark.parametrize(
        "scope, file_name, quotes_mapping_required, quotes_mapping_optional, expected_outcome",
        [
            (
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"column": "Valuation Date"},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.var_field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": 30,
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
            ),
        ],
    )
    def test_load_from_data_frame_quotes_success_from_column_with_numpy_datetime(
        self,
        scope,
        file_name,
        quotes_mapping_required,
        quotes_mapping_optional,
        expected_outcome,
    ) -> None:

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # replace Valuation date values with numpy DateTime values
        data_frame[quotes_mapping_required["quote_id.effective_at"]["column"]] = [
            np.array(["2019-09-01T09:31:22.664"], dtype="datetime64[ns]")
            for _ in range(len(data_frame))
        ]

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=quotes_mapping_required,
            mapping_optional=quotes_mapping_optional,
            file_type="quotes",
        )

        assert sum(
            [len(response.values) for response in responses["quotes"]["success"]]
        ) == len(data_frame)

        assert sum(
            [len(response.failed) for response in responses["quotes"]["success"]]
        ) == 0
