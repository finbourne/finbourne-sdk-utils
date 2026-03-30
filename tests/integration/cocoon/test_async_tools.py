import os
from pathlib import Path
from typing import ClassVar
import pandas as pd

import numpy as np
import concurrent.futures
import pytest

from finbourne_sdk_utils import cocoon as cocoon
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions.api_client_factory import SyncApiClientFactory
from finbourne_sdk_utils import logger


class TestAsyncTools:
    """"
    These tests are to ensure that the Asynchronous Tools work as expected.
    """

    api_factory: ClassVar[SyncApiClientFactory]

    @classmethod
    def setup_class(cls) -> None:
        cls.api_factory = SyncApiClientFactory()
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.skip
    @pytest.mark.parametrize(
        "file_name, number_threads, thread_pool_max_workers",
        [
            (
                "data/holdings-example-large.csv",
                1,
                5,
            ),
            (
                "data/holdings-example-large.csv",
                5,
                5,
            ),
            (
                "data/holdings-example-large.csv",
                5,
                10,
            ),
        ],
        ids=[
            "Standard load with ~700 portfolios in 1 batch with max_threads per batch of 5",
            "Standard load with ~700 portfolios in 5 batches with max_threads per batch of 5",
            "Standard load with ~700 portfolios in 5 batches with max_threads per batch of 10",
        ],
    )
    def test_multiple_threads(self, file_name, number_threads, thread_pool_max_workers):
        """
        This tests different combinations of running load_from_data_frame across multiple threads and configuring
        the max number of workers that each call to load_from_data_frame will use in its thread pool.
        """

        mapping_required = {
            "code": "FundCode",
            "effective_at": "Effective Date",
            "tax_lots.units": "Quantity",
        }

        mapping_optional = {
            "tax_lots.cost.amount": None,
            "tax_lots.cost.currency": "Local Currency Code",
            "tax_lots.portfolio_cost": None,
            "tax_lots.price": None,
            "tax_lots.purchase_date": None,
            "tax_lots.settlement_date": None,
        }

        identifier_mapping = {
            "Isin": "ISIN Security Identifier",
            "Sedol": "SEDOL Security Identifier",
            "Currency": "is_cash_with_currency",
        }

        property_columns = ["Prime Broker"]

        properties_scope = "operations001"

        scope = "prime_broker_test"

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Split the dataframe across the multiple threads
        splitted_arrays = np.array_split(data_frame, number_threads)

        # Create a ThreadPool to run the threads in
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=number_threads)

        # Run each call to load_from_data_frame in a different thread
        futures = [
            executor.submit(
                cocoon.cocoon.load_from_data_frame,
                self.api_factory,
                scope,
                splitted_arrays[i],
                mapping_required,
                mapping_optional,
                "holdings",
                identifier_mapping,
                property_columns,
                properties_scope,
                None,
                True,
                False,
                None,
                False,
                thread_pool_max_workers,
            )
            for i in range(number_threads)
        ]

        # Get the results
        responses = [future.result() for future in futures]

        # Check that the results are as expected
        assert sum([len(response["holdings"]["success"]) for response in responses]) > 0
        assert sum([len(response["holdings"]["success"]) for response in responses]) == len(
            data_frame
        )
        assert sum([len(response["holdings"]["errors"]) for response in responses]) == 0

        assert all(
            isinstance(success_response.version, lusid.Version)
            for response in responses
            for success_response in response["holdings"]["success"]
        )
