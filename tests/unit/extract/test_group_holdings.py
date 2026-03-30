import logging
import os
from finbourne_sdk_utils import logger
from datetime import datetime
import pytest
import pytz

from finbourne_sdk_utils.extract.group_holdings import _join_holdings
from finbourne.sdk.services.lusid.models import (
    PortfolioHolding,
    ModelProperty,
    PropertyValue,
    CurrencyAndAmount,
)

now = datetime.now(pytz.UTC)


class TestCocoonExtractGroupHoldings:
    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    class PortfolioHoldingTemplate(PortfolioHolding):
        """
        This class provides the ability for overriding the template as you see fit
        """

        def __init__(self, **override_init_kwargs):

            default_init_kwargs = {
                "instrument_uid": "LUID_12345678",
                "properties": {
                    "MyProperty": ModelProperty(
                        key="MyProperty", value=PropertyValue(label_value="blah")
                    )
                },
                "holding_type": "B",
                "units": 100,
                "settled_units": 100,
                "cost": CurrencyAndAmount(amount=10000, currency="AUD"),
                "cost_portfolio_ccy": CurrencyAndAmount(amount=0, currency="AUD"),
            }

            init_kwargs = {**default_init_kwargs, **override_init_kwargs}

            super().__init__(**init_kwargs)

    @pytest.mark.parametrize(
        "holdings_to_join, group_by_portfolio, dict_key, expected_outcome",
        [
            (
                {
                    "MyScope : PortfolioA": [PortfolioHoldingTemplate()],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                True,
                None,
                {
                    "MyScope : PortfolioA": [PortfolioHoldingTemplate()],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
            ),
            (
                {
                    "MyScope : PortfolioA": [PortfolioHoldingTemplate()],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "units": 200,
                                "settled_units": 200,
                                "cost": CurrencyAndAmount(amount=20000, currency="AUD"),
                                "properties": {},
                            }
                        )
                    ]
                },
            ),
            (
                {
                    "MyScope : PortfolioA": [
                        PortfolioHoldingTemplate(
                            **{
                                "properties": {
                                    "Instrument/default/Name": ModelProperty(
                                        key="Instrument/default/Name",
                                        value=PropertyValue(label_value="Apple"),
                                    )
                                }
                            }
                        )
                    ],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "units": 200,
                                "settled_units": 200,
                                "cost": CurrencyAndAmount(amount=20000, currency="AUD"),
                                "properties": {
                                    "Instrument/default/Name": ModelProperty(
                                        key="Instrument/default/Name",
                                        value=PropertyValue(label_value="Apple"),
                                    )
                                },
                            }
                        )
                    ]
                },
            ),
            (
                {
                    "MyScope : PortfolioA": [
                        PortfolioHoldingTemplate(
                            **{"cost": CurrencyAndAmount(amount=1500, currency="USD")}
                        )
                    ],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "cost": CurrencyAndAmount(amount=1500, currency="USD"),
                                "properties": {},
                            }
                        ),
                        PortfolioHoldingTemplate(**{"properties": {}}),
                    ]
                },
            ),
            (
                {
                    "MyScope : PortfolioA": [
                        PortfolioHoldingTemplate(
                            **{
                                "cost": CurrencyAndAmount(amount=1500, currency="USD"),
                                "instrument_uid": "LUID_ABD29433",
                            }
                        ),
                        PortfolioHoldingTemplate(),
                    ],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "cost": CurrencyAndAmount(amount=1500, currency="USD"),
                                "instrument_uid": "LUID_ABD29433",
                                "properties": {},
                            }
                        ),
                        PortfolioHoldingTemplate(
                            **{
                                "units": 200,
                                "settled_units": 200,
                                "cost": CurrencyAndAmount(amount=20000, currency="AUD"),
                                "properties": {},
                            }
                        ),
                    ]
                },
            ),
        ]
    )
    def test_join_holdings(
        self, holdings_to_join, group_by_portfolio, dict_key, expected_outcome
    ):

        joined_holdings = _join_holdings(
            holdings_to_join=holdings_to_join,
            group_by_portfolio=group_by_portfolio,
            dict_key=dict_key,
        )

        logging.info(joined_holdings)
        logging.info(expected_outcome)

        actual_dumps = {k: [h.model_dump() for h in v] for k, v in joined_holdings.items()}
        expected_dumps = {k: [h.model_dump() for h in v] for k, v in expected_outcome.items()}
        assert actual_dumps == expected_dumps
