import pytest
from pathlib import Path
from typing import Any, ClassVar, cast
import pandas as pd
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory

from finbourne_sdk_utils.cocoon.seed_sample_data import seed_data
from finbourne_sdk_utils.cocoon.utilities import load_json_file
from finbourne_sdk_utils.cocoon.utilities import create_scope_id
import logging
import random
from unittest.mock import Mock

logger = logging.getLogger()

secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
seed_sample_data_override_csv = Path(__file__).parent.joinpath(
    "data/seed_sample_data/sample_data_override.csv"
)
sample_data_csv = Path(__file__).parent.joinpath(
    "data/seed_sample_data/sample_data.csv"
)


class CocoonSeedDataTestsBase:

    """
    Class description:
    ------------------
    This class creates the tests we use in other classes below.
    We split the file loading and tests into separate classes to improve readability and reuse.
    This class can be extended to your own tests as follows:
    (1) Create a new class with this class as a subclass.
    (2) Load data into LUSID via the setup of new class.
    (3) Run tests against new data.
    """

    api_factory: ClassVar[SyncApiClientFactory]
    scope: ClassVar[str]
    sample_data: ClassVar[pd.DataFrame]

    def test_transactions_from_response(self):
        transactions_from_response = self.api_factory.build(
            lusid.TransactionPortfoliosApi
        ).get_transactions(
            scope=self.scope,
            code=self.sample_data["portfolio_code"].to_list()[0],
            property_keys=[f"Transaction/{self.scope}/strategy"],
        )

        assert (
            set([txn.transaction_id for txn in transactions_from_response.values])
            == set(self.sample_data["txn_id"].to_list())
        )

    def test_portfolio_from_response(self):
        portfolio_from_response = self.api_factory.build(
            lusid.PortfoliosApi
        ).get_portfolio(
            scope=self.scope, code=self.sample_data["portfolio_code"].to_list()[0]
        )

        assert (
            portfolio_from_response.id.code
            == self.sample_data["portfolio_code"].to_list()[0]
        )

    def test_bad_file_type(self):
        with pytest.raises(ValueError) as exc_info:
            seed_data(
                self.api_factory,
                ["bad_file_type"],
                self.scope,
                seed_sample_data_override_csv,
                file_type="csv",
            )

        assert (
            exc_info.value.args[0]
            == "The provided file_type of bad_file_type has no associated mapping"
        )

    def test_instruments_from_response(self):
        # In this test we take a random sample from the dataframe.
        # We need to make a seperate get_instrument request for each item in the sample, so the test might become
        # inefficient if we take the entire dataframe.
        # A sample of approximately 10 should prove the function works as expected.

        instruments_api = self.api_factory.build(lusid.InstrumentsApi)

        random_10_ids = set(
            random.choices(self.sample_data["instrument_id"].to_list(), k=10)
        )
        response_from_random_10 = set(
            [
                response.identifiers["ClientInternal"]
                for response in [
                    instruments_api.get_instrument(
                        identifier_type="ClientInternal", identifier=id
                    )
                    for id in random_10_ids
                ]
            ]
        )

        assert random_10_ids == response_from_random_10

    def test_sub_holding_keys(self):
        holdings_response = self.api_factory.build(
            lusid.TransactionPortfoliosApi
        ).get_holdings(
            scope=self.scope, code=self.sample_data["portfolio_code"].to_list()[0],
        )

        list_of_prop_values = [
            cast(Any, holding.sub_holding_keys)[f"Transaction/{self.scope}/strategy"].value
            for holding in holdings_response.values
        ]
        unique_strategy_labels = set([cast(Any, prop).label_value for prop in list_of_prop_values])

        assert unique_strategy_labels == set(self.sample_data["strategy"].to_list())


class TestSeedDataNoMappingOverrideCSV(CocoonSeedDataTestsBase):
    domain_list: ClassVar[list[str]]
    seed_data_result: ClassVar[dict]

    @classmethod
    def setup_class(cls) -> None:

        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = SyncApiClientFactory()
        cls.sample_data = pd.read_csv(sample_data_csv)

        cls.domain_list = ["portfolios", "instruments", "transactions"]

        cls.seed_data_result = seed_data(
            cls.api_factory,
            cls.domain_list,
            cls.scope,
            sample_data_csv,
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="csv",
        )

    @classmethod
    def teardown_class(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )

    def test_return_dict(self):

        result = self.seed_data_result

        assert isinstance(result, dict)
        assert set(result.keys()) == set(self.domain_list)


class TestSeedDataWithMappingOverrideCSV(CocoonSeedDataTestsBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = SyncApiClientFactory()

        cls.sample_data = pd.read_csv(seed_sample_data_override_csv)

        seed_data(
            cls.api_factory,
            ["portfolios", "instruments", "transactions"],
            cls.scope,
            seed_sample_data_override_csv,
            mappings=dict(
                load_json_file(
                    Path(__file__).parent.joinpath(
                        "data",
                        "seed_sample_data",
                        "seed_sample_data_override.json",
                    )
                )
            ),
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="csv",
        )

    @classmethod
    def teardown_class(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )

    def test_override_with_custom_mapping(self):
        result = seed_data(
            self.api_factory,
            ["portfolios", "instruments", "transactions"],
            self.scope,
            seed_sample_data_override_csv,
            mappings=dict(
                load_json_file(
                    Path(__file__).parent.parent.parent.joinpath(
                        "integration",
                        "cocoon",
                        "data",
                        "seed_sample_data",
                        "seed_sample_data_override.json",
                    )
                )
            ),
            sub_holding_keys=[f"Transaction/{self.scope}/strategy"],
            file_type="csv",
        )
        assert len(result["portfolios"][0]["portfolios"]["success"]) == 1
        assert len(result["instruments"][0]["instruments"]["success"]) == 1
        assert len(result["transactions"][0]["transactions"]["success"]) == 1


class TestSeedDataNoMappingOverrideExcel(CocoonSeedDataTestsBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = SyncApiClientFactory()
        cls.sample_data = pd.read_excel(
            Path(__file__).parent.joinpath(
                "data/seed_sample_data/sample_data_excel.xlsx"
            ),
            engine="openpyxl",
        )

        seed_data(
            cls.api_factory,
            ["portfolios", "instruments", "transactions"],
            cls.scope,
            Path(__file__).parent.joinpath(
                "data/seed_sample_data/sample_data_excel.xlsx"
            ),
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="xlsx",
        )

    @classmethod
    def teardown_class(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )


class TestSeedDataUnsupportedFile:
    scope: ClassVar[str]
    api_factory: ClassVar[Mock]

    @classmethod
    def setup_class(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = Mock()

    def test_bad_file_type(self):
        with pytest.raises(ValueError) as exc_info:
            seed_data(
                self.api_factory,
                ["portfolios", "instruments", "transactions"],
                self.scope,
                Path(__file__).parent.joinpath("data/seed_sample_data/sample_data.xml"),
                file_type="xml",
            )

        assert (
            exc_info.value.args[0]
            == "Unsupported file type, please upload one of the following: ['csv', 'xlsx']"
        )

    def test_inconsistent_file_extensions(self):
        file_extension = "xlsx"

        with pytest.raises(ValueError) as exc_info:
            seed_data(
                self.api_factory,
                ["portfolios", "instruments", "transactions"],
                self.scope,
                seed_sample_data_override_csv,
                file_type=file_extension,
            )

        assert (
            exc_info.value.args[0]
            == f"""Inconsistent file and file extensions passed: {seed_sample_data_override_csv} does not have file extension {file_extension}"""
        )

    def test_file_not_exist(self):
        transaction_file = "data/seed_sample_data/file_not_exist.csv"

        with pytest.raises(FileNotFoundError):
            seed_data(
                self.api_factory,
                ["portfolios", "instruments", "transactions"],
                self.scope,
                transaction_file,
                file_type="csv",
            )


class TestSeedDataPassDataFrame(CocoonSeedDataTestsBase):
    test_dataframe: ClassVar[pd.DataFrame]

    @classmethod
    def setup_class(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = SyncApiClientFactory()

        cls.test_dataframe = pd.read_csv(sample_data_csv)
        cls.sample_data = pd.read_csv(sample_data_csv)

        seed_data(
            cls.api_factory,
            ["portfolios", "instruments", "transactions"],
            cls.scope,
            cls.test_dataframe,
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="DataFrame",
        )

    @classmethod
    def teardown_class(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )
