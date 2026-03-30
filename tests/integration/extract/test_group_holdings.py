import datetime
import os
from pathlib import Path
from typing import ClassVar

import pandas as pd
import pytest
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory

from finbourne_sdk_utils import cocoon as cocoon
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.utilities import create_scope_id
from finbourne_sdk_utils.extract.group_holdings import get_holdings_for_group

# Create the Portfolios, Portfolio Groups and Holdings
_scope = create_scope_id()


class TestExtractGroupHoldings:
    api_factory: ClassVar[SyncApiClientFactory]
    scope: ClassVar[str]

    @classmethod
    def setup_class(cls) -> None:
        cls.api_factory = SyncApiClientFactory()
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))
        cls.scope = _scope

        portfolio_holdings = pd.read_csv(
            Path(__file__).parent.joinpath("./data/holdings-example.csv")
        )
        portfolio_groups = pd.read_csv(
            Path(__file__).parent.joinpath("./data/portfolio-groups.csv")
        )

        # Create portfolios
        response = cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=cls.scope,
            data_frame=portfolio_holdings,
            mapping_required={
                "code": "FundCode",
                "display_name": "FundCode",
                "created": "$2010-10-09T08:00:00Z",
                "base_currency": "$AUD",
            },
            mapping_optional={},
            file_type="portfolios",
            property_columns=[],
        )
        assert len(response["portfolios"]["errors"]) == 0

        # Add holdings
        response = cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=cls.scope,
            data_frame=portfolio_holdings,
            mapping_required={
                "code": "FundCode",
                "effective_at": "Effective Date",
                "tax_lots.units": "Quantity",
            },
            mapping_optional={
                "tax_lots.cost.amount": "Local Market Value",
                "tax_lots.cost.currency": "Local Currency Code",
                "tax_lots.portfolio_cost": None,
                "tax_lots.price": None,
                "tax_lots.purchase_date": None,
                "tax_lots.settlement_date": None,
            },
            file_type="holdings",
            identifier_mapping={
                "ClientInternal": "SEDOL Security Identifier",
                "Currency": "is_cash_with_currency",
            },
            property_columns=[],
            holdings_adjustment_only=True,
        )
        assert len(response["holdings"]["errors"]) == 0, len(
            response["holdings"]["errors"]
        )

        # Create groups
        response = cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=cls.scope,
            data_frame=portfolio_groups,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
                "created": "$2010-10-09T08:00:00Z",
            },
            mapping_optional={
                "values.scope": f"${cls.scope}",
                "values.code": "FundCode",
            },
            file_type="portfolio_group",
            property_columns=[],
        )
        assert len(response["portfolio_groups"]["errors"]) == 0

        # Create group with sub-groups
        group_response = cls.api_factory.build(
            lusid.PortfolioGroupsApi
        ).create_portfolio_group(
            scope=cls.scope,
            create_portfolio_group_request=lusid.CreatePortfolioGroupRequest(
                code="SubGroups",
                display_name="SubGroups",
                created=datetime.datetime(2010, 10, 9, 8, 0, 0, tzinfo=datetime.timezone.utc),
                values=[lusid.ResourceId(scope=cls.scope, code="Portfolio-Y")],
                sub_groups=[lusid.ResourceId(scope=cls.scope, code="ABC12")],
            ),
        )

        assert isinstance(group_response, lusid.PortfolioGroup)

    def check_holdings_correct(self, group_holdings, expected_values, lusid_results_keyed):
        # Check that there are the right number of results and they have the correct keys
        assert set(group_holdings.keys()) == set(expected_values.keys())

        if lusid_results_keyed is not None:
            assert set(group_holdings.keys()) == set(lusid_results_keyed.keys())

        # Iterate over each result
        for portfolio, holdings in group_holdings.items():

            # Key the result by the instrument name
            holdings_by_instrument_name = {
                (holding.properties or {})["Instrument/default/Name"].value.label_value: holding
                for holding in holdings
            }

            # Check that the units and cost are correct according to manual test data
            for instrument_name, holding in holdings_by_instrument_name.items():
                assert holding.cost is not None
                assert float(holding.cost.amount) == float(
                    expected_values[portfolio][instrument_name]["cost"]
                )
                assert float(holding.units) == float(
                    expected_values[portfolio][instrument_name]["units"]
                )
                assert (
                    holding.cost.currency
                    == expected_values[portfolio][instrument_name]["cost.currency"]
                )

                # If this is not grouped by Portfolio, skip checking against LUSID as the merging would have to be
                # done above the API anyway
                if lusid_results_keyed is None:
                    return

                # Check that the units and cost are correct according to LUSID results
                lusid_holding = lusid_results_keyed[portfolio][instrument_name]
                assert lusid_holding.cost is not None
                assert lusid_holding.cost_portfolio_ccy is not None
                assert float(holding.cost.amount) == float(lusid_holding.cost.amount)
                assert float(holding.units) == float(lusid_holding.units)
                assert holding.cost.currency == lusid_holding.cost.currency
                assert (
                    holding.cost_portfolio_ccy.currency
                    == lusid_holding.cost_portfolio_ccy.currency
                )
                assert float(holding.cost_portfolio_ccy.amount) == float(
                    lusid_holding.cost_portfolio_ccy.amount
                )

    @pytest.mark.parametrize(
        "group_code, group_by_portfolio, expected_results",
        [
            (
                "ABC12",
                True,
                {
                    f"{_scope} : Portfolio-X": {
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                    f"{_scope} : Portfolio-Z": {
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                },
            ),
            (
                "SubGroups",
                True,
                {
                    f"{_scope} : Portfolio-X": {
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                    f"{_scope} : Portfolio-Z": {
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                    f"{_scope} : Portfolio-Y": {
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                    },
                },
            ),
            (
                "ABC12",
                False,
                {
                    f"{_scope} : ABC12": {
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "44076868",
                            "cost.currency": "USD",
                            "units": "190842",
                        },
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                    }
                },
            ),
            (
                "SubGroups",
                False,
                {
                    f"{_scope} : SubGroups": {
                        "Amazon": {
                            "cost": "105833442",
                            "cost.currency": "USD",
                            "units": "53196",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "44076868",
                            "cost.currency": "USD",
                            "units": "190842",
                        },
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "4168000",
                            "cost.currency": "USD",
                            "units": "20000",
                        },
                    }
                },
            ),
        ],
        ids=[
            "Grouped by portfolio, no sub-groups",
            "Grouped by portfolio, single sub-group",
            "Merged together, no sub-groups",
            "Merged together, single sub-group",
        ],
    )
    def test_get_holdings_for_group_grouped_by_portfolio(
        self, group_code, group_by_portfolio, expected_results
    ) -> None:

        # Get the group holdings
        group_holdings = get_holdings_for_group(
            api_factory=self.api_factory,
            group_scope=_scope,
            group_code=group_code,
            property_keys=["Instrument/default/Name"],
            group_by_portfolio=group_by_portfolio,
            num_threads=15,
        )

        # If not grouping by Portfolio there is no point checking against LUSID as there is no equivalent
        if not group_by_portfolio:
            # Still check against manually prepared data
            return self.check_holdings_correct(group_holdings, expected_results, None)

        # Otherwise get the result from LUSID
        lusid_results = self.api_factory.build(
            lusid.PortfolioGroupsApi
        ).get_holdings_for_portfolio_group(
            scope=self.scope, code=group_code, property_keys=["Instrument/default/Name"]
        )

        # Key the LUSID result against portfolio and then instrument name
        lusid_results_keyed: dict = {}
        for holding in lusid_results.values:
            portfolio_scope = (holding.properties or {}).get(
                "Holding/default/SourcePortfolioScope"
            )
            portfolio_code = (holding.properties or {}).get(
                "Holding/default/SourcePortfolioId"
            )
            if portfolio_scope is None or portfolio_code is None:
                continue
            if portfolio_scope.value is None or portfolio_code.value is None:
                continue
            portfolio = f"{portfolio_scope.value.label_value} : {portfolio_code.value.label_value}"

            instrument_prop = (holding.properties or {}).get("Instrument/default/Name")
            if instrument_prop is None or instrument_prop.value is None:
                continue
            instrument_name = instrument_prop.value.label_value
            lusid_results_keyed.setdefault(portfolio, {}).setdefault(
                instrument_name, holding
            )

        # Check that the result is as expected
        self.check_holdings_correct(
            group_holdings, expected_results, lusid_results_keyed
        )
