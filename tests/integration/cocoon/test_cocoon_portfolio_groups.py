from pathlib import Path
from typing import ClassVar
import pandas as pd
import json
import finbourne.sdk.services.lusid as lusid
import finbourne.sdk.services.lusid.models as models
from finbourne.sdk.extensions import SyncApiClientFactory
from finbourne.sdk.exceptions import ApiException

from finbourne_sdk_utils import cocoon as cocoon
from finbourne_sdk_utils.cocoon.utilities import create_scope_id
from dateutil.tz import tzutc
import logging
from datetime import datetime

logger = logging.getLogger()


class TestCocoonPortfolioGroup:
    portfolio_scope: ClassVar[str]
    api_factory: ClassVar[SyncApiClientFactory]
    unique_portfolios: ClassVar[list[str]]

    @classmethod
    def setup_class(cls) -> None:

        cls.portfolio_scope = create_scope_id()

        cls.api_factory = SyncApiClientFactory()

        cls.unique_portfolios = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/test_1_pg_create_with_portfolios.csv"
            )
        )["FundCode"].tolist()

        def create_portfolio_model(code):
            model = models.CreateTransactionPortfolioRequest(
                displayName=code,
                code=code,
                baseCurrency="GBP",
                description="Paper transaction portfolio",
                created=datetime(2020, 2, 25, 0, 0, 0, tzinfo=tzutc()),
            )
            return model

        for code in cls.unique_portfolios:
            try:
                cls.api_factory.build(
                    lusid.TransactionPortfoliosApi
                ).create_portfolio(
                    scope=cls.portfolio_scope,
                    create_transaction_portfolio_request=create_portfolio_model(code),
                )
            except ApiException as e:
                if e.status == 404:
                    logger.error(f"The portfolio {code} already exists")

    def log_error_requests_title(self, domain, responses):
        if len(responses.get(domain, {}).get("errors", [])) > 0:
            for error in responses[domain]["errors"]:
                return logger.error(json.loads(error.body)["title"])

    def csv_to_data_frame_with_scope(self, csv, scope):
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(csv))
        data_frame["Scope"] = scope
        return data_frame

    def cocoon_load_from_dataframe(
        self,
        scope,
        data_frame,
        mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
        property_columns=[],
        properties_scope=None,
    ):

        return cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional=mapping_optional,
            file_type="portfolio_group",
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

    def test_01_pg_create_with_portfolios(self) -> None:

        """
        Test description:
        ------------------
        Here we test adding multiple new portfolio groups with multiple portfolios.

        Expected outcome:
        -----------------
        We expect one successful request/response per portfolio group with multiple portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_1_pg_create_with_portfolios.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        #self.log_error_requests_title("portfolio_groups", responses)

        # Test that there is a successful request per line in the dataframe
        assert (
            len(
                [
                    port_group
                    for nested_group in [
                        port_group.portfolios
                        for port_group in responses["portfolio_groups"]["success"]
                    ]
                    for port_group in nested_group
                ]
            )
            == len(data_frame)
        )

        # Test that all the portfolios in the dataframe are in the request
        assert sorted(
            [
                code.to_dict()
                for code in responses["portfolio_groups"]["success"][0].portfolios
            ],
            key=lambda item: item.get("code") or "",
        ) == sorted(
            [
                lusid.ResourceId(
                    code=str(data_frame["FundCode"][1]), scope=str(data_frame["Scope"][1])
                ).to_dict(),
                lusid.ResourceId(
                    code=str(data_frame["FundCode"][0]), scope=str(data_frame["Scope"][0])
                ).to_dict(),
            ],
            key=lambda item: item.get("code") or "",
        )

        assert responses["portfolio_groups"]["success"][1].portfolios == [
            lusid.ResourceId(
                code=str(data_frame["FundCode"][2]), scope=str(data_frame["Scope"][2])
            )
        ]

        # Assert that by no unmatched_identifiers are returned in the response for portfolio_groups
        assert not responses["portfolio_groups"].get("unmatched_identifiers", False)

    def test_02_pg_create_with_no_portfolio(self) -> None:

        """
        Test description:
        -----------------
        Here we test adding one new portfolio group with no portfolios.

        Expected outcome:
        -----------------
        We expect one successful new portfolio group with no portfolios.

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_2_pg_create_with_no_portfolio.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check that the portfolio group is created
        assert (
            len(
                [
                    port_group.id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            )
            == len(data_frame)
        )

        # Check that the correct portfolio group code is used
        assert responses["portfolio_groups"]["success"][0].id == lusid.ResourceId(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
        )

        # Check that the portfolio group request has now portfolios
        assert len(responses["portfolio_groups"]["success"][0].portfolios) == 0

    def test_03_pg_create_multiple_groups_no_portfolio(self) -> None:

        """
        Test description:
        -----------------
        Here we test adding multiple new portfolio group with no portfolios.

        Expected outcome
        -----------------
        We expect successful requests/responses for multiple new portfolio groups.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_3_pg_create_multiple_groups_no_portfolio.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check that there is a requet per portfolio group
        assert (
            len(
                [
                    port_group.id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            )
            == len(data_frame)
        )

        # Check that the portfolio group code matches the code in the dataframe
        assert responses["portfolio_groups"]["success"][1].id == lusid.ResourceId(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[1]
        )

    def test_04_pg_create_with_portfolio_not_exist(self):

        """
        Test description:
        -----------------
        Here we test attempting to add a portfolio which does not exist to a portfolio group.

        Expected outcome:
        -----------------
        We expect the entire request to fail with a PortfolioNotFound error.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_4_pg_create_with_portfolio_not_exist.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check that LUSID cannot find the portfolio
        assert (
            json.loads(responses["portfolio_groups"]["errors"][0].body)["name"]
            == "PortfolioNotFound"
        )

        # Check there are no successful requests
        assert len(responses["portfolio_groups"]["success"]) == 0

    def test_05_pg_create_with_duplicate_portfolios(self):

        """
        Test description:
        -----------------
        Here we test attempting to add two of the same portfolios to a portfolio group.

        Expected result:
        ----------------
        We expect that each unique portfolio gets added and duplicates should be ignored.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_5_pg_create_with_duplicate_portfolios.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        data_frame.drop_duplicates(inplace=True)

        # Check that there is a request for each unique portfolio
        assert (
            len(
                [
                    port_group
                    for nested_group in [
                        port_group.portfolios
                        for port_group in responses["portfolio_groups"]["success"]
                    ]
                    for port_group in nested_group
                ]
            )
            == len(data_frame)
        )

        # Check that a request is generated with unqiue portfolio only
        assert sorted(
            [
                code.to_dict()
                for code in responses["portfolio_groups"]["success"][0].portfolios
            ],
            key=lambda item: item.get("code") or "",
        ) == sorted(
            [
                lusid.ResourceId(
                    code=str(data_frame["FundCode"][0]), scope=str(data_frame["Scope"][0])
                ).to_dict(),
                lusid.ResourceId(
                    code=str(data_frame["FundCode"][1]), scope=str(data_frame["Scope"][1])
                ).to_dict(),
            ],
            key=lambda item: item.get("code") or "",
        )

    def test_06_pg_create_duplicate_port_group(self):

        """
        Test description:
        -----------------
        Here we test create two of the same portfolio groups

        Expected results
        -----------------
        We expect one successful requesy for the portfolio group

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_6_pg_create_duplicate_port_group.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check for one successful request
        assert len(responses["portfolio_groups"]["success"]) == 1

        # Check the successful request has same code as dataframe portfolio group
        assert responses["portfolio_groups"]["success"][0].id == lusid.ResourceId(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
        )

    def test_07_pg_create_with_properties(self) -> None:

        """
        Test description:
        -----------------
        Here we test creating a portfolio group with properties.

        Expected output:
        ----------------
        The response contains the upserted properties.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_7_pg_create_with_properties.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope,
            data_frame=data_frame,
            property_columns=["location", "MifidFlag"],
            properties_scope=test_case_scope,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        response_with_properties = self.api_factory.build(
            lusid.PortfolioGroupsApi
        ).get_group_properties(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0],
        )

        assert response_with_properties.properties == {
            "PortfolioGroup/"
            + test_case_scope
            + "/location": lusid.ModelProperty(
                key="PortfolioGroup/" + test_case_scope + "/location",
                value=lusid.PropertyValue(label_value="UK"),
                effective_from=datetime.min.replace(tzinfo=tzutc()),
                effective_until=datetime.max.replace(tzinfo=tzutc()),
            ),
            "PortfolioGroup/"
            + test_case_scope
            + "/MifidFlag": lusid.ModelProperty(
                key="PortfolioGroup/" + test_case_scope + "/MifidFlag",
                value=lusid.PropertyValue(label_value="Y"),
                effective_from=datetime.min.replace(tzinfo=tzutc()),
                effective_until=datetime.max.replace(tzinfo=tzutc()),
            ),
        }

    def test_08_pg_add_bad_portfolio(self):

        """
        Description:
        ------------
        Here we test add a portfolio which does not exist to a current portfolio group.

        Expected results:
        -----------------
        The portfolio group is returned without the portfolios added.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_8_pg_add_bad_portfolio.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        assert len(responses["portfolio_groups"]["errors"]) == 0

        assert responses["portfolio_groups"]["success"][0].id == lusid.ResourceId(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
        )

    def test_09_pg_add_duplicate_portfolio(self) -> None:

        """
        Description:
        ------------
        Here we test adding duplicate portfolios to a portfolio group.

        Expected outcome:
        -----------------
        We expect the one portfolio to be added.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_9_pg_add_duplicate_portfolio.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        assert responses["portfolio_groups"]["success"][0].portfolios[0] == lusid.ResourceId(
            code=str(data_frame["FundCode"][0]), scope=str(data_frame["Scope"][0])
        )

    def test_10_pg_add_no_new_portfolio(self) -> None:

        """
        Test description:
        ------------
        Here we test adding an existing portfolio to portfolio group.

        Expected result:
        ----------------
        The portfolio group response should be returned with one unmodified portfolio.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_10_pg_add_no_new_portfolio.csv",
            self.portfolio_scope,
        )

        port_group_request = lusid.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
            values=[
                lusid.ResourceId(
                    code=str(data_frame["FundCode"][0]), scope=self.portfolio_scope
                )
            ],
        )

        self.api_factory.build(lusid.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        assert responses["portfolio_groups"]["success"][0].portfolios[0] == lusid.ResourceId(
            code=str(data_frame["FundCode"][0]), scope=str(data_frame["Scope"][0])
        )

    def test_11_pg_add_bad_and_good_portfolios(self):

        """
        Test description:
        -----------------
        Here we test updating a portfolio group with good and bad portfolios.

        Expected result:
        -----------------
        Good portfolios should be added and bad ones not added.

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_11_pg_add_bad_and_good_portfolios.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        remove_dupe_df = data_frame[~data_frame["FundCode"].str.contains("BAD_PORT")]

        assert sorted(
            [
                code.to_dict()
                for code in responses["portfolio_groups"]["success"][0].portfolios
            ],
            key=lambda item: item.get("code") or "",
        ) == sorted(
            [
                lusid.ResourceId(
                    code=remove_dupe_df["FundCode"].tolist()[0],
                    scope=self.portfolio_scope,
                ).to_dict(),
                lusid.ResourceId(
                    code=remove_dupe_df["FundCode"].tolist()[1],
                    scope=self.portfolio_scope,
                ).to_dict(),
            ],
            key=lambda item: item.get("code") or "",
        )

    def test_12_pg_add_portfolios_different_scopes(self) -> None:

        """
        Test description:
        -----------------
        Here we test adding portfolios with multiple scopes.

        Expected outcome:
        -----------------
        Request should be successful - returned with portfolios with multiple scopes.

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_12_pg_add_portfolios_different_scopes.csv",
            self.portfolio_scope,
        )

        port_scope_for_test = create_scope_id()
        self.api_factory.build(lusid.TransactionPortfoliosApi).create_portfolio(
            scope=port_scope_for_test,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name=data_frame["FundCode"][0],
                code=data_frame["FundCode"][0],
                base_currency="GBP",
            ),
        )

        port_group_request = lusid.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
            values=[
                lusid.ResourceId(
                    code=str(data_frame["FundCode"][0]), scope=port_scope_for_test
                )
            ],
        )

        self.api_factory.build(lusid.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        assert all(
            [
                id in responses["portfolio_groups"]["success"][0].portfolios
                for id in [
                    lusid.ResourceId(
                        code=str(data_frame["FundCode"][1]), scope=str(data_frame["Scope"][1])
                    ),
                    lusid.ResourceId(
                        code=str(data_frame["FundCode"][0]), scope=port_scope_for_test
                    ),
                    lusid.ResourceId(
                        code=str(data_frame["FundCode"][0]), scope=str(data_frame["Scope"][0])
                    ),
                    lusid.ResourceId(
                        code=str(data_frame["FundCode"][2]), scope=str(data_frame["Scope"][2])
                    ),
                ]
            ]
        )
