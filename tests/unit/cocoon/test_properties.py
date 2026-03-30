import os
import pytest
import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.exceptions import ApiException
import finbourne_sdk_utils.cocoon as cocoon
import pandas as pd
import numpy as np
from finbourne_sdk_utils import logger
from .mock_api_factory import MockApiFactory


class TestCocoonProperties:
    @classmethod
    def setup_class(cls) -> None:
        # Use a mock of the lusid.ApiClientFactory
        cls.api_factory = MockApiFactory( )
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "property_key, expected_outcomes, throws_exception",
        [
            ("Instrument/default/Figi", [True, "string"], False),
            ("Instrument/default/PropertyThatDoesNotExist", [False, None], False),
            ("Transaction/default/TradeToPortfolioRate", [True, "number"], False),
            ("Transaction/Operations/Strategy", [True, "string"], False),
            ("Holding/Operations/Currency", [True, "currency"], False),
            ("Instrument/default/Forbidden", [False, None], False),
        ],
    )
    def test_check_property_definitions_exist_in_scope_single(
        self, property_key, expected_outcomes, throws_exception
    ) -> None:
        """
        Tests that checking for a property definition in a single scope works as expected. The call to LUSID
        is mocked here

        :param str property_key: The property key to check for
        :param list[bool, str] expected_outcomes: Whether or not the property is expected to exist and if it does its type

        :return: None
        """
        try:
            (
                property_existence,
                property_type,
            ) = cocoon.properties.check_property_definitions_exist_in_scope_single(
                api_factory=self.api_factory, property_key=property_key
            )

            assert property_existence == expected_outcomes[0]
            assert property_type == expected_outcomes[1]
        except ApiException:
            assert throws_exception

    @pytest.mark.parametrize(
        "scope, domain, data_frame, property_columns, expected_outcome",
        [
            # Check for two properties which don't exist and one that does with a type of string
            (
                "default",
                "Instrument",
                pd.DataFrame(
                    data={
                        "Figi": ["BBG009QYHGN8", "BBG00JRKP9P0", "BBG009QYHGN8"],
                        "MoodysCreditRating": ["A2", "A1", "A2"],
                        "Age": [30, 70, 15],
                    },
                ),
                ["Figi", "MoodysCreditRating", "Age"],
                [
                    ["MoodysCreditRating", "Age"],
                    pd.DataFrame(
                        data={
                            "Figi": ["BBG009QYHGN8", "BBG00JRKP9P0", "BBG009QYHGN8"],
                            "MoodysCreditRating": ["A2", "A1", "A2"],
                            "Age": [30, 70, 15],
                        },
                    ),
                ],
            ),
            # Check for one property that exists with a currency type and one that does not exist
            (
                "Operations",
                "Holding",
                pd.DataFrame(
                    data={
                        "Currency": ["GBP", "USD", "AUD"],
                        "Notional": [True, False, True],
                    }
                ),
                ["Currency", "Notional"],
                [
                    ["Notional"],
                    pd.DataFrame(
                        data={
                            "Currency": ["GBP", "USD", "AUD"],
                            "Notional": [True, False, True],
                        }
                    ),
                ],
            ),
        ],
    )
    def test_check_property_definitions_exist_in_scope(
        self, scope, domain, data_frame, property_columns, expected_outcome
    ) -> None:
        """
        Tests that checking of the existence of a set of property definitions in a scope works as expected

        :param str scope: The scope of the properties to check for
        :param str domain: The domain of the properties to check for
        :param pd.DataFrame data_frame: The dataframe containing the column headers and data to use to determine data types
        :param list[str]: The list of property columns to check for
        :param list[list[str], pd.DataFrame] expected_outcome: The expected result
        :return: None
        """

        (
            missing_columns,
            updated_data_frame,
        ) = cocoon.properties.check_property_definitions_exist_in_scope(
            api_factory=self.api_factory,
            domain=domain,
            data_frame=data_frame,
            target_columns=property_columns,
            column_to_scope={column: scope for column in property_columns},
        )

        assert len(missing_columns) == len(set(missing_columns))

        assert set(missing_columns) == set(expected_outcome[0])

        assert updated_data_frame.equals(expected_outcome[1])

    @pytest.mark.parametrize(
        "scope, domain, data_frame, missing_property_columns, expected_outcome",
        [
            # Test that a missing property is created as expected
            (
                "CreditRating",
                "Instrument",
                pd.DataFrame(
                    data=[
                        {
                            "instrument_name": "GlobalCreditFund",
                            "lookthrough_scope": "SingaporeBranch",
                            "lookthrough_code": "PORT_12490FKS9",
                            "format": "json",
                            "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                            "Moodys": "A2",
                        }
                    ]
                ),
                ["Moodys"],
                [
                    {"Moodys": "Instrument/CreditRating/Moodys"},
                    pd.DataFrame(
                        data=[
                            {
                                "instrument_name": "GlobalCreditFund",
                                "lookthrough_scope": "SingaporeBranch",
                                "lookthrough_code": "PORT_12490FKS9",
                                "format": "json",
                                "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                                "Moodys": "A2",
                            }
                        ]
                    ),
                ],
            ),
            # Test that a property with all null values has a definition created as a string not a number
            (
                "CreditRating",
                "Instrument",
                pd.DataFrame(
                    data=[
                        {
                            "instrument_name": "GlobalCreditFund",
                            "lookthrough_scope": "SingaporeBranch",
                            "lookthrough_code": "PORT_12490FKS9",
                            "format": "json",
                            "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                            "S&P": np.nan,
                        }
                    ]
                ),
                ["S&P"],
                [
                    {"S&P": "Instrument/CreditRating/SandP"},
                    pd.DataFrame(
                        data=[
                            {
                                "instrument_name": "GlobalCreditFund",
                                "lookthrough_scope": "SingaporeBranch",
                                "lookthrough_code": "PORT_12490FKS9",
                                "format": "json",
                                "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                                "S&P": np.nan,
                            }
                        ],
                        dtype=object,
                    ),
                ],
            ),
        ],
    )
    def test_create_property_definitions_from_file(
        self, scope, domain, data_frame, missing_property_columns, expected_outcome
    ) -> None:
        """

        :param str scope: The scope to create the property definitions in
        :param str domain: The domain to create the property definitions in
        :param pd.Series data_frame_dtypes: The dataframe dtypes to add definitions for
        :return: dict property_key_mapping: A mapping of data_frame columns to property keys
        :param [dict, pd.DataFrame] expected_outcome: The expected outcome

        :return: None
        """

        (
            property_key_mapping,
            data_frame_updated,
        ) = cocoon.properties.create_property_definitions_from_file(
            api_factory=self.api_factory,
            domain=domain,
            data_frame=data_frame,
            missing_property_columns=missing_property_columns,
            column_to_scope={column: scope for column in missing_property_columns},
        )

        assert property_key_mapping == expected_outcome[0]

        assert data_frame_updated.equals(expected_outcome[1])

    @pytest.mark.parametrize(
        "row, scope, domain, dtypes, expected_outcome",
        [
            # Test creating property values in the instrument domain
            (
                pd.Series(data=["A2", "A-"], index=["Moodys", "S&P"]),
                "CreditRating",
                "Instrument",
                pd.Series(data=["object", "object"], index=["Moodys", "S&P"]),
                [
                    lusid.ModelProperty(
                        key="Instrument/CreditRating/Moodys",
                        value=lusid.PropertyValue(label_value="A2"),
                    ),
                    lusid.ModelProperty(
                        key="Instrument/CreditRating/SandP",
                        value=lusid.PropertyValue(label_value="A-"),
                    ),
                ],
            ),
            # Test creating property values in the transaction domain, including a metric value property
            (
                pd.Series(
                    data=["TD_1241247", 30], index=["TraderId", "Rebalancing_Interval"]
                ),
                "Operations",
                "Transaction",
                pd.Series(
                    data=["object", "float64"],
                    index=["TraderId", "Rebalancing_Interval"],
                ),
                {
                    "Transaction/Operations/TraderId": lusid.PerpetualProperty(
                        key="Transaction/Operations/TraderId",
                        value=lusid.PropertyValue(label_value="TD_1241247"),
                    ),
                    "Transaction/Operations/Rebalancing_Interval": lusid.PerpetualProperty(
                        key="Transaction/Operations/Rebalancing_Interval",
                        value=lusid.PropertyValue(
                            metric_value=lusid.MetricValue(value=30)
                        ),
                    ),
                },
            ),
        ],
    )
    def test_create_property_values(
        self, row, scope, domain, dtypes, expected_outcome
    ) -> None:
        """

        :param pd.Series row:
        :param str scope:
        :param str domain:
        :param pd.Series dtypes:
        :param list[lusid.PerpetualProperty] expected_outcome:

        :return: None
        """

        property_values = cocoon.properties.create_property_values(
            row=row, column_to_scope={}, scope=scope, domain=domain, dtypes=dtypes
        )

        assert property_values == expected_outcome

    @pytest.mark.parametrize(
        "description, partial_keys, properties_scope, domain, expected_outcome",
        [
            (
                "Just a code",
                ["Strategy"],
                "Traders",
                "Transaction",
                ["Transaction/Traders/Strategy"],
            ),
            (
                "A scope and code",
                ["Signal/Strategy"],
                "Traders",
                "Transaction",
                ["Transaction/Signal/Strategy"],
            ),
            (
                "A full key",
                ["Portfolio/Signal/Strategy"],
                "Traders",
                "Transaction",
                ["Portfolio/Signal/Strategy"],
            ),
            (
                "A scope which is the same as the domain",
                ["Transaction/Strategy"],
                "Traders",
                "Transaction",
                ["Transaction/Transaction/Strategy"],
            ),
        ],
    )
    def test_infer_full_property_keys(
        self, description, partial_keys, properties_scope, domain, expected_outcome
    ):

        full_keys = cocoon.properties._infer_full_property_keys(
            partial_keys=partial_keys, properties_scope=properties_scope, domain=domain
        )

        assert full_keys == expected_outcome

    def test_create_property_definitions_with_leading_nulls(self):
        """
        Tests that property definitions are created with correct data types
        even when columns have leading null values.

        The function should find the first non-null value and infer the type
        from that, rather than defaulting to string for columns with null headers.
        """
        # Create a DataFrame with leading nulls in both string and numeric columns
        data_frame = pd.DataFrame(
            data=[
                {
                    "Name": np.nan,
                    "Count": np.nan,
                    "Rating": np.nan,
                },
                {
                    "Name": np.nan,
                    "Count": np.nan,
                    "Rating": np.nan,
                },
                {
                    "Name": "Product A",  # First non-null string value
                    "Count": 42,  # First non-null integer value
                    "Rating": 4.5,  # First non-null float value
                },
                {
                    "Name": "Product B",
                    "Count": 100,
                    "Rating": 3.8,
                },
            ]
        )

        missing_property_columns = ["Name", "Count", "Rating"]
        column_to_scope = {col: "default" for col in missing_property_columns}

        # Call the function
        property_key_mapping, updated_data_frame = cocoon.properties.create_property_definitions_from_file(
            api_factory=self.api_factory,
            domain="Instrument",
            data_frame=data_frame,
            missing_property_columns=missing_property_columns,
            column_to_scope=column_to_scope,
        )

        # Validate that properties were created
        assert len(property_key_mapping) == 3
        assert "Name" in property_key_mapping
        assert "Count" in property_key_mapping
        assert "Rating" in property_key_mapping

        # Validate property keys were created correctly
        assert property_key_mapping["Name"] == "Instrument/default/Name"
        assert property_key_mapping["Count"] == "Instrument/default/Count"
        assert property_key_mapping["Rating"] == "Instrument/default/Rating"

        # Validate that the DataFrame data types were inferred correctly
        # Count should be int64 or float64 (numeric)
        assert str(updated_data_frame["Count"].dtype) in ["int64", "float64"]
        # Rating should be float64 (numeric)
        assert str(updated_data_frame["Rating"].dtype) == "float64"
        # Name should remain object (string)
        assert str(updated_data_frame["Name"].dtype) == "object"
