import os
import pytest
from pathlib import Path
import pandas as pd
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory

from finbourne_sdk_utils import cocoon as cocoon
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.utilities import create_scope_id


class TestCocoonReferencePortfolios:
    @classmethod
    def setup_class(cls) -> None:

        cls.api_factory = SyncApiClientFactory()

        cls.portfolios_api = cls.api_factory.build(lusid.PortfoliosApi)
        cls.unique_id = create_scope_id()
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))
        cls.scope = "ModelFundTest"
        cls.file_name = "data/reference-portfolio/reference-test.csv"

    @pytest.mark.parametrize(
        "file_name, mapping_required, mapping_optional, property_columns",
        [
            (
                "data/reference-portfolio/reference-test.csv",
                {"code": "FundCode", "display_name": "display_name"},
                {
                    "description": "description",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                [],
            ),
            (
                "data/reference-portfolio/reference-test.csv",
                {"code": "FundCode", "display_name": "display_name"},
                {
                    "description": "description",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                ["strategy", "custodian"],
            ),
            (
                "data/reference-portfolio/reference-test-duplicates.csv",
                {"code": "FundCode", "display_name": "display_name"},
                {
                    "description": "description",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                [],
            ),
        ],
    )
    def test_load_from_data_frame_attributes_and_properties_success(
        self, file_name, mapping_required, mapping_optional, property_columns,
    ) -> None:
        """
        Test that a reference portfolio can be loaded successfully

        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param list[str] property_columns: The columns to create properties for
        :param any expected_outcome: The expected outcome

        :return: None
        """

        unique_id = create_scope_id()
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))
        data_frame.drop_duplicates(inplace=True)
        data_frame["FundCode"] = data_frame["FundCode"] + "-" + unique_id

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="reference_portfolio",
            identifier_mapping={},
            property_columns=property_columns,
            properties_scope=self.scope,
            sub_holding_keys=[],
        )

        # Check that the count of portfolios uploaded equals count of portfolios in DataFrame

        assert len(responses["reference_portfolios"]["success"]) == len(data_frame)

        # Assert that by no unmatched_identifiers are returned in the response for reference_portfolios
        assert not responses["reference_portfolios"].get("unmatched_identifiers", False)

        # Check that the portfolio IDs of porfolios uploaded matches the IDs of portfolios in DataFrame

        response_codes = [
            response.id.code
            if isinstance(response, lusid.Portfolio)
            else response.origin_portfolio_id.code
            for response in responses["reference_portfolios"]["success"]
        ]

        assert response_codes == list(data_frame[mapping_required["code"]].values)

        # Check that properties get added to portfolio

        for portfolio in responses["reference_portfolios"]["success"]:

            get_portfolio = self.portfolios_api.get_portfolio(
                scope=portfolio.id.scope,
                code=portfolio.id.code,
                property_keys=[
                    f"Portfolio/{self.scope}/strategy",
                    f"Portfolio/{self.scope}/custodian",
                ],
            )

            property_keys_from_params = [
                f"Portfolio/{self.scope}/{code}"
                for code in property_columns
                if len(property_columns) > 0
            ]

            assert sorted([prop for prop in (get_portfolio.properties or {})]) == sorted(property_keys_from_params)

    def test_portfolio_missing_attribute(self):

        unique_id = create_scope_id()
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(self.file_name))
        data_frame["FundCode"] = data_frame["FundCode"] + "-" + unique_id

        mapping_required = {"code": "FundCode"}
        mapping_optional = {
            "description": "description",
            "created": "created",
            "base_currency": "base_currency",
        }

        with pytest.raises(ValueError) as exc_info:

            cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope=self.scope,
                data_frame=data_frame,
                mapping_required=mapping_required,
                mapping_optional=mapping_optional,
                file_type="reference_portfolio",
                identifier_mapping={},
                property_columns=[],
                properties_scope=self.scope,
                sub_holding_keys=[],
            )

        assert exc_info.value.args[0] == """The required attributes {'display_name'} are missing from the mapping. Please
                             add them."""
