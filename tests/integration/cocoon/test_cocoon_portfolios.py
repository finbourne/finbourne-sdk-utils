import os
import uuid
from pathlib import Path
from typing import ClassVar

import pandas as pd
import pytest
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory
from finbourne.sdk.exceptions import ApiException

from finbourne_sdk_utils import cocoon as cocoon
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.utilities import create_scope_id


class TestCocoonPortfolios:
    api_factory: ClassVar[SyncApiClientFactory]

    @classmethod
    def setup_class(cls) -> None:
        cls.api_factory = SyncApiClientFactory()
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope, sub_holding_keys, sub_holdings_key_scope, expected_sub_holdings_keys",
        [
            (
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
                None,
                None,
            ),
            (
                "prime_broker_test",
                "data/metamorph_portfolios-large.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
                None,
                None,
            ),
            (
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": "$AverageCost"},
                {},
                ["base_currency"],
                "operations001",
                None,
                None,
                None,
            ),
            (
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id()}",
                None,
                None,
                None,
            ),
            (
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                ["Strategy"],
                None,
                ["Transaction/operations001/Strategy"],
            ),
            (
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                ["Strategy", "Broker"],
                None,
                ["Transaction/operations001/Strategy", "Transaction/operations001/Broker"],
            ),
            (
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id(use_uuid=True)}",
                ["Trader/Strategy"],
                None,
                ["Transaction/Trader/Strategy"],
            ),
            (
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id(use_uuid=True)}",
                ["Transaction/Trader/Strategy"],
                None,
                ["Transaction/Trader/Strategy"],
            ),
            (
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                ["Strategy"],
                "accountview007",
                ["Transaction/accountview007/Strategy"],
            ),
        ],
        ids=[
            "Standard load",
            "Standard load with ~700 portfolios",
            "Add in some constants",
            "Standard load with unique properties scope to ensure property creation",
            "Standard load with a single sub-holding-key",
            "Standard load with a multiple sub-holding-keys in a unique scope",
            "Standard load with a sub-holding-key with scope specified",
            "Standard load with a sub-holding-key with domain and scope specified",
            "Standard load with a single sub-holding-key in a different scope",
        ],
    )
    def test_load_from_data_frame_a_portfolios_success(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
        sub_holdings_key_scope,
        expected_sub_holdings_keys,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            sub_holding_keys_scope=sub_holdings_key_scope,
        )

        assert len(responses["portfolios"]["success"]) == len(data_frame)

        response_codes = [
            response.id.code
            if isinstance(response, lusid.Portfolio)
            else response.origin_portfolio_id.code
            for response in responses["portfolios"]["success"]
        ]

        assert response_codes == list(data_frame[mapping_required["code"]].values)

        # Assert that by no unmatched_identifiers are returned in the response for portfolios
        assert not responses["portfolios"].get("unmatched_identifiers", False)

        if expected_sub_holdings_keys is None:
            return

        for portfolio_code in response_codes:
            portfolio_details = self.api_factory.build(
                lusid.TransactionPortfoliosApi
            ).get_details(scope=scope, code=portfolio_code)

            assert set(portfolio_details.sub_holding_keys or []) == set(expected_sub_holdings_keys)

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope, sub_holding_keys",
        [
            (
                "p rime_broker_test",  # note the space between p and r, this should cause a server side response error
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
            ),
        ],
        ids=["ApiException due to invalid scope"],
    )
    def test_load_from_data_frame_a_portfolios_failure(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        file_type = "portfolios"

        responses = (
            cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope=scope,
                data_frame=data_frame,
                mapping_required=mapping_required,
                mapping_optional=mapping_optional,
                file_type=file_type,
                identifier_mapping=identifier_mapping,
                property_columns=property_columns,
                properties_scope=properties_scope,
                sub_holding_keys=sub_holding_keys,
            ),
        )
        assert len(responses) == 1
        assert len(responses[0][file_type]["errors"]) == 3

        assert isinstance(responses[0][file_type]["errors"][0], ApiException)
        assert isinstance(responses[0][file_type]["errors"][1], ApiException)
        assert isinstance(responses[0][file_type]["errors"][2], ApiException)

        # body is a str, not a type
        # avoided a hard coded message check as this likely to break when error message is changed
        assert "scope" in responses[0][file_type]["errors"][0].body
        assert "scope" in responses[0][file_type]["errors"][1].body
        assert "scope" in responses[0][file_type]["errors"][2].body

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_columns, properties_scope",
        [
            (
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {},
                {},
                ["base_currency"],
                "operations0011",
            ),
        ],
        ids=["Standard load"],
    )
    def test_properties_list_strings(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

        for property_column in property_columns:
            response = self.api_factory.build(
                lusid.PropertyDefinitionsApi
            ).get_property_definition(
                domain="Portfolio", scope=properties_scope, code=property_column,
            )
            assert f"Portfolio/{properties_scope}/{property_column}" == response.key

    @pytest.mark.parametrize(
        "scope, file_name, mapping_required, mapping_optional, identifier_mapping, property_column, properties_scope, expected_property_scope, expected_property_code",
        [
            (
                f"prime_broker_test__{uuid.uuid4()}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {},
                {},
                {"source": "base_currency"},
                "operations0011",
                "operations0011",
                "base_currency",
            ),
            (
                f"prime_broker_test_{uuid.uuid4()}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {},
                {},
                {"source": "base_currency", "target": "base_currency2"},
                "operations0011",
                "operations0011",
                "base_currency2",
            ),
            (
                f"prime_broker_test_{uuid.uuid4()}",
                "data/metamorph_portfolios-unique.csv",
                {"code": "FundCode", "display_name": "display_name", "created": "created", "base_currency": "base_currency"},
                {},
                {},
                {"source": "base_currency", "target": "base_currency2", "scope": "foo"},
                "operations0011",
                "foo",
                "base_currency2",
            ),
        ],
        ids=["Source only", "Source and target", "Scope"],
    )
    def test_properties_dicts(
        self,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_column,
        properties_scope,
        expected_property_scope,
        expected_property_code,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=[property_column],
            properties_scope=properties_scope,
        )

        response = self.api_factory.build(
            lusid.PropertyDefinitionsApi
        ).get_property_definition(
            domain="Portfolio",
            scope=expected_property_scope,
            code=expected_property_code,
        )
        assert f"Portfolio/{expected_property_scope}/{expected_property_code}" == response.key

        for code in data_frame["FundCode"].values:
            response = self.api_factory.build(
                lusid.PortfoliosApi
            ).get_portfolio_properties(scope=scope, code=code)
            assert (response.properties or {}).get(
                f"Portfolio/{expected_property_scope}/{expected_property_code}"
            ) is not None

    @pytest.mark.parametrize(
        "property_columns, expected_error_message",
        [
            (
                [{"foo": "bar"}],
                "The value [{'foo': 'bar'}] provided in property_columns is invalid. "
                "{'foo': 'bar'} does not contain the mandatory 'source' key.",
            ),
            (
                [1],
                "The value [1] provided in property_columns is invalid. "
                "1 is not a string or dictionary.",
            ),
        ],
        ids=["Invalid dictionary", "Non string or dictionary"],
    )
    def test_invalid_properties(self, property_columns, expected_error_message) -> None:
        with pytest.raises(ValueError) as exc_info:
            cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope="foo",
                data_frame=pd.DataFrame(),
                mapping_required={},
                mapping_optional={},
                file_type="portfolios",
                identifier_mapping={},
                property_columns=property_columns,
                properties_scope="bar",
            )

        assert expected_error_message == str(exc_info.value)
