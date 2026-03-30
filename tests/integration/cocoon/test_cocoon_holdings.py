import os
import json
from pathlib import Path
import pandas as pd
from datetime import datetime
import pytz

from finbourne_sdk_utils import cocoon as cocoon
import pytest
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory
from finbourne.sdk.exceptions import ApiException
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.utilities import create_scope_id


def holding_instrument_identifiers(fake1=False, fake2=False):
    if fake1:
        fake1 = {
            "Instrument/default/Sedol": "FAKESEDOL1",
            "Instrument/default/Isin": "FAKEISIN1",
        }
    else:
        fake1 = None

    if fake2:
        fake2 = {
            "Instrument/default/Sedol": "FAKESEDOL2",
            "Instrument/default/Isin": "FAKEISIN2",
        }
    else:
        fake2 = None

    return [instrument for instrument in [fake1, fake2] if instrument]


def extract_unique_identifiers_from_holdings_response(response):
    unmatched_instruments = [
        holding_adjustment.instrument_identifiers
        for holding_adjustment in response["holdings"].get("unmatched_items", [])
    ]

    return list(
        map(
            dict,
            set(
                tuple(sorted(identifiers.items()))
                for identifiers in unmatched_instruments
            ),
        )
    )


class TestCocoonHoldings:
    @classmethod
    def setup_class(cls) -> None:
        
        cls.api_factory = SyncApiClientFactory()
        
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope, sub_holding_keys, sub_holding_key_scope, holdings_adjustment_only, expected_outcome",
        [
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-large.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                True,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date-string-index.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-index.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001_{create_scope_id()}",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": 3000,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {"default": "2019-10-08"},
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {"column": "Effective Date"},
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {
                        "column": "Effective Date",
                        "default": "2019-10-08",
                    },
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Security Description"],
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Security Description", "Prime Broker"],
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["swap"],
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001_{create_scope_id()}",
                ["Prime Broker"],
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Prime Broker"],
                f"accountview_{create_scope_id()}",
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Quantity"],
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": 1,
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": 1,
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
            (
                "prime_broker_test",
                "data/holdings-example-unique-date-one-unmatched-instrument.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.Version,
            ),
        ]
    )
    def test_load_from_data_frame_holdings_success(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
        sub_holding_key_scope,
        holdings_adjustment_only,
        expected_outcome,
    ) -> None:
        """
        Test that holdings can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list[str] property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param list sub_holding_keys: The sub holding keys to populate on the adjustments as transaction properties
        :param bool holdings_adjustment_only: Whether to use the adjust_holdings api call rather than set_holdings when
               working with holdings
        :param any expected_outcome: The expected outcome

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="holdings",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            holdings_adjustment_only=holdings_adjustment_only,
            sub_holding_keys_scope=sub_holding_key_scope,
        )

        assert len(responses["holdings"]["success"]) > 0

        assert len(responses["holdings"]["errors"]) == 0, responses["holdings"]["errors"]

        # Assert that by default no unmatched_identifiers are returned in the response
        assert not responses["holdings"].get("unmatched_identifiers", False)

        assert all(
                isinstance(success_response.version, expected_outcome)
                for success_response in responses["holdings"]["success"]
            )

    @pytest.mark.parametrize(
        "file_name, sub_holding_keys, holdings_adjustment_only, expected_holdings_in_response, expected_unmatched_identifiers",
        [
            (
                "data/holdings-example-unique-date.csv",
                None,
                False,
                0,
                holding_instrument_identifiers(fake1=False, fake2=False),
            ),
            (
                "data/holdings-example-unique-date-one-unmatched-instrument.csv",
                None,
                False,
                1,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-unique-date-incorrect-sedol-correct_isin_clientInternal_should_resolve.csv",
                None,
                False,
                0,
                holding_instrument_identifiers(fake1=False, fake2=False),
            ),
            (
                "data/holdings-example-unique-date-one-unmatched-instrument-duplicated-in-two-adjustments.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-unique-date-two-unmatched-instruments-separate-adjustments.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-unique-date-two-unmatched-instruments-single-adjustment.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-one-unmatched-instrument-in-two-adjustments-one-portfolio-two-dates.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-same-date-one-unmatched-instrument-duplicated-in-two-adjustments-two-portfolios.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-two-dates-two-unmatched-instruments-separate-adjustments-two-portfolios.csv",
                None,
                False,
                4,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-one-portfolio-one-shk-one-date-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-one-portfolio-one-shk-two-dates-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-one-portfolio-two-shks-one-date-one-instrument.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-one-portfolio-two-shks-one-date-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-one-portfolio-two-shks-two-dates-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-one-portfolio-two-shks-two-dates-one-instrument.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-one-portfolio-two-shks-two-dates-one-instrument.csv",
                ["Security Description"],
                True,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-one-portfolio-one-shk-two-dates-two-instruments.csv",
                ["Security Description"],
                True,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-unique-date-two-unmatched-instruments-single-adjustment.csv",
                None,
                True,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ),
            (
                "data/holdings-example-unique-date-one-unmatched-instrument.csv",
                None,
                True,
                1,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ),
            (
                "data/holdings-example-unique-date-two-unmatched-instruments-single-adjustment_three_unique_ids.csv",
                None,
                True,
                2,
                [
                    {
                        "Instrument/default/Sedol": "FAKESEDOL1",
                        "Instrument/default/Isin": "FAKEISIN1",
                    },
                    {
                        "Instrument/default/Sedol": "FAKESEDOL1",
                        "Instrument/default/Isin": "FAKEISIN2",
                    },
                ],
            ),
        ]
    )
    def test_load_from_data_frame_holdings_success_with_unmatched_items(
        self,
        file_name,
        sub_holding_keys,
        holdings_adjustment_only,
        expected_holdings_in_response,
        expected_unmatched_identifiers,
    ) -> None:
        """
        Test that holdings can be loaded successfully

        :param str file_name: The name of the test data file
        :param list[str] sub_holding_keys: The column names for any sub-holding-keys
        :param bool holdings_adjustment_only: Whether to use the set or adjust holding api
        :param int expected_holdings_in_response: The number of expected holdings in the response
        :param list[instrument_identifiers] expected_unmatched_identifiers: A list of expected instrument_identifiers

        :return: None
        """
        # Unchanged vars that have no need to be passed via param (they would count as duplicate lines)
        scope = f"unmatched_holdings_test_{create_scope_id()}"
        mapping_required = {
            "code": "FundCode",
            "effective_at": "Effective Date",
            "tax_lots.units": "Quantity",
        }
        mapping_optional = {"tax_lots.cost.currency": "Local Currency Code"}
        identifier_mapping = {
            "Isin": "ISIN Security Identifier",
            "Sedol": "SEDOL Security Identifier",
            "Currency": "is_cash_with_currency",
            "ClientInternal": "Client Internal",
        }
        property_columns = ["Prime Broker"]
        properties_scope = "operations001"
        sub_holding_key_scope = None
        return_unmatched_items = True
        expected_outcome = lusid.Version

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Create instruments that should resolve (for tests expecting instruments to exist)
        # This is specifically for the test with incorrect SEDOL but correct ISIN and ClientInternal
        if "incorrect-sedol-correct_isin_clientInternal_should_resolve" in file_name:
            instruments_df = pd.DataFrame([{
                "name": "12% BD REDEEM 26/07/2014 CRC 1000000",
                "isin": "CRMADAPB2335",
                "clientInternal": "3940007-CRBNV-CRC"
            }])
            
            instrument_response = cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope=scope,
                data_frame=instruments_df,
                mapping_required={
                    "name": "name"
                },
                mapping_optional={},
                identifier_mapping={
                    "Isin": "isin",
                    "ClientInternal": "clientInternal"
                },
                file_type="instruments"
            )
            
            # Verify instruments were created successfully
            assert len(instrument_response.get("instruments").get("errors")) == 0

        # Create the portfolios
        portfolio_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "FundCode",
                "display_name": "FundCode",
                "base_currency": "Local Currency Code",
            },
            file_type="portfolio",
            mapping_optional={"created": "Created Date"},
        )

        # Assert that all portfolios were created without any issues
        assert len(portfolio_response.get("portfolios").get("errors")) == 0

        # Load in the holdings adjustments
        holding_responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="holdings",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            holdings_adjustment_only=holdings_adjustment_only,
            sub_holding_keys_scope=sub_holding_key_scope,
            return_unmatched_items=return_unmatched_items,
        )

        assert len(holding_responses["holdings"]["success"]) > 0

        assert len(holding_responses["holdings"]["errors"]) == 0

        # Assert that the number of holdings returned are as expected
        assert len(holding_responses["holdings"]["unmatched_items"]) == expected_holdings_in_response

        # Assert that the holdings returned only contain the instrument identifiers that did not resolve
        actual = extract_unique_identifiers_from_holdings_response(holding_responses)
        assert len(actual) == len(expected_unmatched_identifiers)
        assert all(item in expected_unmatched_identifiers for item in actual)

        assert all(
                isinstance(success_response.version, expected_outcome)
                for success_response in holding_responses["holdings"]["success"]
            )

        # Delete the portfolios at the end of the test
        for portfolio in portfolio_response.get("portfolios").get("success"):
            self.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
                scope=scope, code=portfolio.id.code
            )

    def test_return_unmatched_holding_handles_empty_holdings_exception_in_lusid(self):
        """
        Test that the get holdings call to LUSID handles cases where there are no holdings present for a
        given portfolio code and effective at combination.

        :return: None
        """
        portfolio_code_and_scope = "set_holdings_test"
        effective_at_datetime_for_portfolio = datetime(2020, 1, 1, tzinfo=pytz.utc)
        base_currency = "GBP"
        effective_at_str_for_holdings = "2020-01-01T12:00:00"
        code_tuple = (portfolio_code_and_scope, effective_at_str_for_holdings)

        # Create an empty portfolio for the test, but handle situations where the portfolio already exists
        try:
            portfolio_setup_response = self.api_factory.build(
                lusid.TransactionPortfoliosApi
            ).create_portfolio(
                scope=portfolio_code_and_scope,
                create_transaction_portfolio_request=lusid.CreateTransactionPortfolioRequest(
                    display_name=portfolio_code_and_scope,
                    description=portfolio_code_and_scope,
                    code=portfolio_code_and_scope,
                    created=effective_at_datetime_for_portfolio,
                    base_currency=base_currency,
                ),
            )

            # Assert that the portfolio was created successfully
            assert isinstance(portfolio_setup_response, lusid.Portfolio)

        except ApiException as e:
            if "PortfolioWithIdAlreadyExists" not in str(e.body):
                raise e

        # Call the return_unmatched_holdings method where there are no holdings
        unmatched_holdings_response = cocoon.cocoon.return_unmatched_holdings(
            api_factory=self.api_factory,
            scope=portfolio_code_and_scope,
            code_tuple=code_tuple,
        )

        # Assert that the unmatched holdings response does not throw an error, and returns an empty list
        assert [] == unmatched_holdings_response

        # Delete the portfolio at the end of the test
        self.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            scope=portfolio_code_and_scope, code=portfolio_code_and_scope
        )

    @pytest.mark.parametrize(
        "file_name, sub_holding_keys, error_name, skip_portfolio",
        [
            (
                "data/holdings-example-duplicate-shk.csv",
                ["Security Description"],
                "DuplicateSubHoldingKeysProvided",
                False,
            ),
            (
                "data/holdings-example-single-holding.csv",
                ["Security Description"],
                "PortfolioNotFound",
                True,
            ),
        ],
    )
    def test_failed_load_from_data_frame_holdings_returns_useful_errors_and_skips_validation_logic(
        self, file_name, sub_holding_keys, error_name, skip_portfolio,
    ) -> None:
        """
        Test that a failed holding upload is handled gracefully by the whole load_from_data_frame function.
        If any upload causes an error the validation logic will not run and all the errors will be returned
        by load_from_data_frame.

        :param str file_name: The name of the test data file
        :param list[str] sub_holding_keys: The column names for any sub-holding-keys
        :param str error_name: The name of the ApiException that is hit by this error
        :param bool skip_portfolio: Flag whether to skip the creation of a test portfolio

        :return: None
        """
        # Unchanged vars that have no need to be passed via param (they would count as duplicate lines)
        scope = f"unmatched_holdings_test_{create_scope_id()}"
        mapping_required = {
            "code": "FundCode",
            "effective_at": "Effective Date",
            "tax_lots.units": "Quantity",
        }
        mapping_optional = {"tax_lots.cost.currency": "Local Currency Code"}
        identifier_mapping = {
            "Isin": "ISIN Security Identifier",
            "Sedol": "SEDOL Security Identifier",
            "Currency": "is_cash_with_currency",
            "ClientInternal": "Client Internal",
        }
        property_columns = ["Prime Broker"]
        properties_scope = "operations001"
        sub_holding_key_scope = None
        return_unmatched_items = True
        holdings_adjustment_only = False
        failed_unmatched_items_check = [
            "Please resolve all upload errors to check for unmatched items."
        ]

        portfolio_response = None

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        if not skip_portfolio:
            # Create the portfolios
            portfolio_response = cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope=scope,
                data_frame=data_frame,
                mapping_required={
                    "code": "FundCode",
                    "display_name": "FundCode",
                    "base_currency": "Local Currency Code",
                },
                file_type="portfolio",
                mapping_optional={"created": "Created Date"},
            )

            # Assert that all portfolios were created without any issues
            assert len(portfolio_response.get("portfolios").get("errors")) == 0

        # Load in the holdings adjustments
        holding_responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="holdings",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            holdings_adjustment_only=holdings_adjustment_only,
            sub_holding_keys_scope=sub_holding_key_scope,
            return_unmatched_items=return_unmatched_items,
        )

        assert len(holding_responses["holdings"]["success"]) == 0

        assert len(holding_responses["holdings"]["errors"]) == 1

        # Assert that the ApiError thrown by LUSID is what is expected, given the input data
        assert json.loads(holding_responses["holdings"]["errors"][0].body)["name"] == error_name

        # Assert that there is no 'unmatched_item' field if the input data resulted in no successful uploads
        assert holding_responses["holdings"].get("unmatched_items") == failed_unmatched_items_check

        if not skip_portfolio:
            # Delete the portfolios at the end of the test
            if portfolio_response is None:
                raise Exception("Portfolio response cannot be None if skip_portfolio is False")
            for portfolio in portfolio_response.get("portfolios").get("success"):
                self.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
                    scope=scope, code=portfolio.id.code
                )
