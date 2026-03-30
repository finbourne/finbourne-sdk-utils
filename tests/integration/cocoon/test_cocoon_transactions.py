import datetime
import os
import uuid
from pathlib import Path
from typing import ClassVar
import pandas as pd

import pytest

from finbourne_sdk_utils import cocoon as cocoon
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory
from finbourne.sdk.exceptions import ApiException
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.utilities import create_scope_id

client_internal = "Instrument/default/ClientInternal"
sedol = "Instrument/default/Sedol"
name = "Instrument/default/Name"


class MockTransaction:
    def __init__(self, transaction_id, instrument_identifiers):
        self.transaction_id = transaction_id
        self.instrument_identifiers = instrument_identifiers


def transaction_and_instrument_identifiers(trd_0003=False, trd_0004=False):
    if trd_0003:
        trd_0003 = MockTransaction(
            transaction_id="trd_0003",
            instrument_identifiers={
                client_internal: "THIS_WILL_NOT_RESOLVE_1",
                sedol: "FAKESEDOL1",
                name: "THIS_WILL_NOT_RESOLVE_1",
            },
        )
    else:
        trd_0003 = None

    if trd_0004:
        trd_0004 = MockTransaction(
            transaction_id="trd_0004",
            instrument_identifiers={
                client_internal: "THIS_WILL_NOT_RESOLVE_2",
                sedol: "FAKESEDOL2",
                name: "THIS_WILL_NOT_RESOLVE_2",
            },
        )
    else:
        trd_0004 = None

    return [transaction for transaction in [trd_0003, trd_0004] if transaction]


def dict_for_comparison(list_of_transactions):
    return {
        transaction.transaction_id: transaction.instrument_identifiers
        for transaction in list_of_transactions
    }


class TestCocoonTransactions:
    api_factory: ClassVar[SyncApiClientFactory]

    @classmethod
    def setup_class(cls) -> None:
        cls.api_factory = SyncApiClientFactory()
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope, batch_size, expected_outcome",
        [
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "operations001",
                None,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "operations001",
                None,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                None,
                None,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "$GlobalCreditFund",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                None,
                None,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "$GlobalCreditFund",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                None,
                2,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "$GlobalCreditFund",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                f"prime_broker_test_{create_scope_id()}",
                2,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions-with-nulls.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trans_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "operations001",
                None,
                lusid.Version,
            ),
        ],
        ids=[
            "Test standard transaction load",
            "Add in some constants",
            "Pass in None for some of properties_scope which accepts this",
            "Pass a constant for the portfolio code",
            "Try with a small batch",
            "Try with a small batch & random scope to ensure property creation",
            "Test standard transaction load with nulls for optional parameters",
        ],
    )
    def test_load_from_data_frame_transactions_success(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        batch_size,
        expected_outcome,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            batch_size=batch_size,
        )

        assert len(responses["transactions"]["success"]) > 0

        assert len(responses["transactions"]["errors"]) == 0, [
            (error.status, error.reason, error.body)
            for error in responses["transactions"]["errors"]
        ]

        # Assert that by default no unmatched_identifiers are returned in the response
        assert not responses["transactions"].get("unmatched_identifiers", False)

        assert all(
            isinstance(success_response.version, expected_outcome)
            for success_response in responses["transactions"]["success"]
        )

    @pytest.mark.parametrize(
        "scope, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope, expected_property_scope, expected_property_code",
        [
            (
                f"prime_broker_test_dict_{uuid.uuid4()}",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["location_region"],
                "operations001",
                "operations001",
                "location_region",
            ),
            (
                f"prime_broker_test_dict_{uuid.uuid4()}",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                [
                    {
                        "scope": "foo",
                        "source": "location_region",
                        "target": "My_location",
                    }
                ],
                "operations001",
                "foo",
                "My_location",
            ),
        ],
        ids=[
            "Test standard transaction load",
            "Test standard transaction load with scope",
        ],
    )
    def test_properties_dicts(
        self,
        scope,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        expected_property_scope,
        expected_property_code,
    ) -> None:
        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath("data/global-fund-combined-transactions.csv")
        )

        # Create the portfolio
        portfolio_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "display_name": "portfolio_code",
                "base_currency": "$GBP",
            },
            file_type="portfolio",
            mapping_optional={
                "created": f"${str(data_frame['transaction_date'].min())}"
            },
        )

        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            batch_size=None,
        )

        transactions_from_response = self.api_factory.build(
            lusid.TransactionPortfoliosApi
        ).get_transactions(
            scope=scope,
            code="GlobalCreditFund",
            property_keys=[
                f"Transaction/{expected_property_scope}/{expected_property_code}"
            ],
            from_transaction_date="2019-09-01T00:00Z",
            to_transaction_date="2019-09-12T00:00Z",
        )

        for tx in transactions_from_response.values:
            assert (tx.properties or {}).get(
                f"Transaction/{expected_property_scope}/{expected_property_code}"
            ) is not None, tx

        # Delete the portfolio at the end of the test
        for portfolio in (portfolio_response.get("portfolios") or {}).get("success") or []:
            self.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
                scope=scope, code=portfolio.id.code
            )

    @pytest.mark.parametrize(
        "file_name, return_unmatched_items, expected_unmatched_transactions",
        [
            (
                "data/global-fund-combined-transactions.csv",
                True,
                [],
            ),
            (
                "data/global-fund-combined-transactions-unresolved-instruments.csv",
                True,
                [
                    "unresolved_tx01",
                    "unresolved_tx02",
                ],
            ),
        ],
        ids=[
            "Test standard transaction load",
            "Test standard transaction load with two unresolved instruments",
        ],
    )
    def test_load_from_data_frame_transactions_success_with_correct_unmatched_identifiers(
        self,
        file_name,
        return_unmatched_items,
        expected_unmatched_transactions,
    ) -> None:
        # Unchanged vars that have no need to be passed via param (they would count as duplicate lines)
        scope = "prime_broker_test"
        mapping_required = {
            "code": "portfolio_code",
            "transaction_id": "id",
            "type": "transaction_type",
            "transaction_date": "transaction_date",
            "settlement_date": "settlement_date",
            "units": "units",
            "transaction_price.price": "transaction_price",
            "transaction_price.type": "price_type",
            "total_consideration.amount": "amount",
            "total_consideration.currency": "trade_currency",
        }
        mapping_optional = {"transaction_currency": "trade_currency"}
        identifier_mapping = {
            "Isin": "isin",
            "Figi": "figi",
            "ClientInternal": "client_internal",
            "Currency": "currency_transaction",
        }
        property_columns = ["exposure_counterparty", "compls", "val", "location_region"]
        properties_scope = "operations001"
        batch_size = None
        expected_outcome = lusid.Version

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            batch_size=batch_size,
            return_unmatched_items=return_unmatched_items,
        )

        assert len(responses["transactions"]["success"]) > 0

        assert len(responses["transactions"]["errors"]) == 0

        # Assert that the unmatched_transactions returned are as expected for each case
        # First check the count of transactions
        assert len(responses["transactions"].get("unmatched_items", False)) == len(
            expected_unmatched_transactions
        )
        # Then check the transaction ids are the ones expected
        assert sorted(
            [
                transaction.transaction_id
                for transaction in responses["transactions"].get("unmatched_items", [])
            ]
        ) == sorted(expected_unmatched_transactions)

        assert all(
            isinstance(success_response.version, expected_outcome)
            for success_response in responses["transactions"]["success"]
        )

    def test_return_unmatched_transactions_extracts_relevant_transactions_and_instruments(
        self,
    ):
        scope = "unmatched_transactions_test"
        code = "MIS_INST_FUND"

        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/transactions_unmatched_instruments.csv"
            )
        )

        # Create a test portfolio
        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "display_name": "portfolio_name",
                "base_currency": "base_currency",
            },
            file_type="portfolio",
            mapping_optional={"created": "$2000-01-01"},
        )

        # Upsert some transactions; one with known id, four with unknown ids
        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "transaction_id": "txn_id",
                "type": "txn_type",
                "transaction_date": "txn_trade_date",
                "settlement_date": "txn_settle_date",
                "units": "txn_units",
                "transaction_price.price": "txn_price",
                "transaction_price.type": "$Price",
                "total_consideration.amount": "txn_consideration",
                "total_consideration.currency": "currency",
            },
            file_type="transaction",
            identifier_mapping={
                "ClientInternal": "instrument_id",
                "Sedol": "sedol",
                "Ticker": "ticker",
                "Name": "name",
            },
            mapping_optional={},
        )

        # Call the return_unmatched_transactions method for dates that include two of the four unmatched
        response_focused = cocoon.cocoon.return_unmatched_transactions(
            self.api_factory,
            scope,
            code,
            from_transaction_date="2020-01-01",
            to_transaction_date="2020-01-03",
        )

        # Call the return_unmatched_transactions method for dates that include four of the four unmatched
        response_wide = cocoon.cocoon.return_unmatched_transactions(
            self.api_factory,
            scope,
            code,
            from_transaction_date="2000-01-01",
            to_transaction_date="2050-01-01",
        )
        assert len(response_wide) == 4

        # Assert that there are only two values returned
        assert len(response_focused) == 2
        # Assert that the transaction ids and instrument identifiers from the returned transactions match expectations
        response_dict = {
            transaction.transaction_id: transaction.instrument_identifiers
            for transaction in response_focused
        }
        assert response_dict.get("trd_0003") == {
            client_internal: "THIS_WILL_NOT_RESOLVE_1",
            sedol: "FAKESEDOL1",
            name: "THIS_WILL_NOT_RESOLVE_1",
        }
        assert response_dict.get("trd_0004") == {
            client_internal: "THIS_WILL_NOT_RESOLVE_2",
            sedol: "FAKESEDOL2",
            name: "THIS_WILL_NOT_RESOLVE_2",
        }

        # Delete the portfolio at the end of the test
        self.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            scope=scope, code=code
        )

    @pytest.mark.parametrize(
        "data_frame_path, unmatched_transactions, expected_filtered_unmatched_transactions",
        [
            (
                "data/transactions_unmatched_instruments_all_in_upload.csv",
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
            ),
            (
                "data/transactions_unmatched_instruments_none_in_upload.csv",
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
                transaction_and_instrument_identifiers(trd_0003=False, trd_0004=False),
            ),
            (
                "data/transactions_unmatched_instruments_some_in_upload.csv",
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=False),
            ),
        ],
        ids=[
            "All returned unmatched transactions were in the upload",
            "No returned unmatched transactions were in the upload",
            "Some of the returned unmatched transactions were in the upload",
        ],
    )
    def test_filter_unmatched_transactions_method_only_returns_transactions_originally_present_in_dataframe(
        self,
        data_frame_path,
        unmatched_transactions,
        expected_filtered_unmatched_transactions,
    ):
        # Load the dataframe
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(data_frame_path))

        # Filter the transactions to remove ones that were not part of the upload
        filtered_unmatched_transactions = cocoon.cocoon.filter_unmatched_transactions(
            data_frame=data_frame,
            mapping_required={"transaction_id": "txn_id"},
            unmatched_transactions=unmatched_transactions,
        )

        # Assert that the transaction ids and identifiers from the transactions returned match up to the expected ones
        assert set(dict_for_comparison(filtered_unmatched_transactions)) == set(
            dict_for_comparison(expected_filtered_unmatched_transactions)
        )

    def test_filter_unmatched_transactions_can_paginate_responses_for_2001_transactions_returned(
        self,
    ):
        """
        The GetTransactions API will only return up to 2,000 transactions per request. This test is to verify that
        LPT can handle responses of greater length, expected to be split across multiple pages.
        """
        scope = "unmatched_transactions_2k_test"
        identifier_mapping = {
            "Isin": "isin",
            "Figi": "figi",
            "ClientInternal": "client_internal",
            "Currency": "currency_transaction",
        }
        mapping_required = {
            "code": "portfolio_code",
            "transaction_id": "id",
            "type": "transaction_type",
            "transaction_date": "transaction_date",
            "settlement_date": "settlement_date",
            "units": "units",
            "transaction_price.price": "transaction_price",
            "transaction_price.type": "price_type",
            "total_consideration.amount": "amount",
            "total_consideration.currency": "trade_currency",
        }
        mapping_optional = {"transaction_currency": "trade_currency"}

        # Load the dataframe
        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/global-fund-combined-transaction_2001.csv"
            )
        )

        # Create the portfolio
        portfolio_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "display_name": "portfolio_code",
                "base_currency": "$GBP",
            },
            file_type="portfolio",
            mapping_optional={"created": "created_date"},
        )

        # Assert that the portfolio was created without any issues
        assert len((portfolio_response.get("portfolios") or {}).get("errors") or []) == 0

        # Load in the transactions
        transactions_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            batch_size=None,
            return_unmatched_items=True,
        )

        assert len(transactions_response["transactions"]["success"]) > 0
        assert len(transactions_response["transactions"]["errors"]) == 0

        # Assert that the unmatched_items returned are as expected
        assert (
            len(transactions_response["transactions"].get("unmatched_items", []))
            == 2001
        )

        # Delete the portfolio at the end of the test
        for portfolio in (portfolio_response.get("portfolios") or {}).get("success") or []:
            self.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
                scope=scope, code=portfolio.id.code
            )

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope, batch_size, sub_holding_keys, expected_sub_holdings_keys",
        [
            (
                "load_dataframe_test",
                "data/no-subholding-keys-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "load_dataframe_test",
                None,
                ["SHK_data"],
                ["Transaction/load_dataframe_test/SHK_data"],
            ),
        ],
        ids=["Test standard transaction load"],
    )
    def test_load_from_dataframe_non_existent_subholding_keys(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        batch_size,
        sub_holding_keys,
        expected_sub_holdings_keys,
    ):
        """
        This checks whether load_from_data_frame creates subholding keys for transactions when they don't already exist.
        """

        # create the apis to reduce repeat definitions
        portfolios_api = self.api_factory.build(lusid.PortfoliosApi)
        transaction_portfolio_api = self.api_factory.build(
            lusid.TransactionPortfoliosApi
        )

        # make sure the portfolio doesn't already exist in scope
        portfolios = portfolios_api.list_portfolios_for_scope(scope).values

        for portfolio in portfolios:
            portfolios_api.delete_portfolio(scope, portfolio.id.code)

        # create the portfolio we're going to use
        transaction_portfolio_api.create_portfolio(
            scope,
            lusid.CreateTransactionPortfolioRequest(
                displayName="test_load_from_dataframe_non_existent_subholding_keys portfolio",
                code="no-SHK",
                baseCurrency="GBP",
                created=datetime.datetime(2017, 6, 22, tzinfo=datetime.timezone.utc),
            ),
        )

        # load the data
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # load in the transactions along with the sub-holding keys
        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            batch_size=batch_size,
            sub_holding_keys=sub_holding_keys,
        )

        # we get the details from the portfolio
        portfolio_details = transaction_portfolio_api.get_details(
            scope=scope, code="no-SHK"
        )

        # get the transactions that we just loaded in
        transactions = transaction_portfolio_api.get_transactions(
            scope=scope, code="no-SHK"
        )

        # check that the sub holding key is a property of the two transactions
        assert (
            "Transaction/load_dataframe_test/SHK_data"
            in (transactions.values[0].properties or {}).keys()
        )
        assert (
            "Transaction/load_dataframe_test/SHK_data"
            in (transactions.values[1].properties or {}).keys()
        )

        # check that the property is a sub-holding key in the portfolio
        assert set(portfolio_details.sub_holding_keys or []) == set(
            expected_sub_holdings_keys
        )

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, transactions_commit_mode, expected_outcome",
        [
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                None,
                None,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                "Not_a_valid_commit_mode",
                None,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                "Partial",
                9,
            ),
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                "Atomic",
                9,
            ),
        ],
        ids=[
            "Test standard transaction load with no commit mode",
            "Test standard transaction load with incorrect commit mode",
            "Test standard transaction load with partial commit mode",
            "Test standard transaction load with atomic commit mode",
        ],
    )
    def test_load_from_data_frame_transactions_with_commit_mode(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        transactions_commit_mode,
        expected_outcome,
    ) -> None:
        # Arrange
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Act
        if transactions_commit_mode is None or transactions_commit_mode not in (
            "Atomic",
            "Partial",
        ):
            with pytest.raises(KeyError):
                cocoon.cocoon.load_from_data_frame(
                    api_factory=self.api_factory,
                    scope=scope,
                    data_frame=data_frame,
                    mapping_required=mapping_required,
                    mapping_optional=mapping_optional,
                    file_type="transactions_with_commit_mode",
                    identifier_mapping=identifier_mapping,
                )
            return

        response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions_with_commit_mode",
            transactions_commit_mode=transactions_commit_mode,
            identifier_mapping=identifier_mapping,
        )

        # Assert
        assert (
            len(response["transactions_with_commit_modes"]["success"][0].values)
            == expected_outcome
        )

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, transactions_commit_mode, expected_outcome",
        [
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions-with-failed-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                "Atomic",
                0,
            ),
        ],
        ids=["Test failed load with atomic commit mode"],
    )
    def test_load_from_data_frame_transactions_with_atomic_commit_mode_expect_failures(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        transactions_commit_mode,
        expected_outcome,
    ) -> None:
        # Arrange
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Act
        response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions_with_commit_mode",
            transactions_commit_mode=transactions_commit_mode,
            identifier_mapping=identifier_mapping,
        )

        # Assert
        assert (
            len(response["transactions_with_commit_modes"]["success"]) == expected_outcome
        )

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, transactions_commit_mode, expected_outcome",
        [
            (
                "prime_broker_test",
                "data/global-fund-combined-transactions-with-failed-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                "Partial",
                8,
            ),
        ],
        ids=["Test failed load with partial commit mode"],
    )
    def test_load_from_data_frame_transactions_with_partial_commit_mode_expect_failures(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        transactions_commit_mode,
        expected_outcome,
    ) -> None:
        # Arrange
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Act
        response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions_with_commit_mode",
            transactions_commit_mode=transactions_commit_mode,
            identifier_mapping=identifier_mapping,
        )

        # Assert
        assert (
            len(response["transactions_with_commit_modes"]["success"][0].values)
            == expected_outcome
        )

    @classmethod
    def teardown_class(cls):
        # remove portfolios/properties created in test_load_from_dataframe_non_existent_subholding_keys
        try:
            cls.api_factory.build(
                lusid.PropertyDefinitionsApi
            ).delete_property_definition(
                "Transaction", "load_dataframe_test", "SHK_data"
            )
        except ApiException as e:
            if "domain" not in str(e.body) and "PropertyNotDefined" not in str(e.body):
                raise e

        try:
            cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
                "load_dataframe_test", "no-SHK"
            )
        except ApiException as e:
            if "PortfolioNotFound" not in str(e.body):
                raise e
