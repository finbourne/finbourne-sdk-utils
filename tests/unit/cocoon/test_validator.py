import os
import pytest
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.validator import Validator


class TestCocoonValidator:
    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    @pytest.mark.parametrize(
        "value, value_name, allowed_values",
        [
            ("Portfolio", "file_type", ["Portfolio", "Transaction"]),
        ],
    )
    def test_check_allowed_value_success(self, value, value_name, allowed_values):

        Validator(value, value_name).check_allowed_value(allowed_values)

    @pytest.mark.parametrize(
        "value, value_name, allowed_values, expected_exception",
        [
            ("Reference", "file_type", ["Portfolio", "Transaction"], ValueError),
            ("Reference", "file_type", [], ValueError),
            (1, "file_type", ["Portfolio", "Transaction"], ValueError),
            (
                ["Portfolio", "Transaction"],
                "file_type",
                ["Portfolio", "Transaction"],
                ValueError,
            ),
            (
                {"type": "portfolio"},
                "file_type",
                ["Portfolio", "Transaction"],
                ValueError,
            ),
        ],
    )
    def test_check_allowed_value_exception(
        self, value, value_name, allowed_values, expected_exception
    ):

        with pytest.raises(expected_exception):
            Validator(value, value_name).check_allowed_value(allowed_values)

    @pytest.mark.parametrize(
        "value, value_name, expected_outcome",
        [
            ("Transactions", "file_type", "Transaction"),
            ("Transactionss", "file_type", "Transaction"),
            ("Transaction", "file_type", "Transaction"),
            (1, "file_type", 1),
        ],
    )
    def test_make_singular(self, value, value_name, expected_outcome):

        singular = Validator(value, value_name).make_singular()

        assert isinstance(singular, Validator)

        assert singular.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, expected_outcome",
        [
            ("TRANSACTIONS", "file_type", "transactions"),
            ("TrAnSaCTIons", "file_type", "transactions"),
            ("transactions", "file_type", "transactions"),
            (1, "file_type", 1),
        ],
    )
    def test_make_lower(self, value, value_name, expected_outcome):

        lower_case = Validator(value, value_name).make_lower()

        assert isinstance(lower_case, Validator)

        assert lower_case.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, default, expected_outcome",
        [
            (None, "batch_size", 10, 10),
            (None, "batch_size", None, None),
            (3, "batch_size", 10, 3),
        ],
    )
    def test_set_default_value_if_none(
        self, value, value_name, default, expected_outcome
    ):

        updated_value = Validator(value, value_name).set_default_value_if_none(default)

        assert isinstance(updated_value, Validator)

        assert updated_value.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, override_flag, override_value, expected_outcome",
        [
            (10, "batch_size", True, 20, 20),
            (10, "batch_size", False, 20, 10),
            (10, "batch_size", "Portfolio" == "Transaction", 20, 10),
        ],
    )
    def test_override_value(
        self, value, value_name, override_flag, override_value, expected_outcome
    ):

        updated_value = Validator(value, value_name).override_value(
            override_flag, override_value
        )

        assert isinstance(updated_value, Validator)

        assert updated_value.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, expected_outcome",
        [
            (
                {"type": None, "code": "portfolio_code"},
                "optional_mapping",
                {"code": "portfolio_code"},
            ),
            (
                {
                    "type": {"column": None, "default": "Reference"},
                    "code": "portfolio_code",
                },
                "required_mapping",
                {
                    "type": {"column": None, "default": "Reference"},
                    "code": "portfolio_code",
                },
            ),
            (
                {"type": "Buy", "code": "portfolio_code"},
                "optional_mapping",
                {"type": "Buy", "code": "portfolio_code"},
            ),
        ],
    )
    def test_discard_dict_keys_none_value(self, value, value_name, expected_outcome):

        update_dict = Validator(value, value_name).discard_dict_keys_none_value()

        assert isinstance(update_dict, Validator)

        assert update_dict.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, expected_outcome",
        [
            (
                {"type": None, "code": "portfolio_code"},
                "optional_mapping",
                [None, "portfolio_code"],
            ),
            (
                {
                    "type": {"column": None, "default": "Reference"},
                    "code": "portfolio_code",
                },
                "required_mapping",
                [{"column": None, "default": "Reference"}, "portfolio_code"],
            ),
            (
                {"type": "Buy", "code": "portfolio_code"},
                "optional_mapping",
                ["Buy", "portfolio_code"],
            ),
        ],
    )
    def test_get_dict_values(self, value, value_name, expected_outcome):

        dict_values = Validator(value, value_name).get_dict_values()

        assert isinstance(dict_values, Validator)

        assert dict_values.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, first_character, expected_outcome",
        [
            (["$Code", "$Buy", "Sell"], "code_list", "$", ["Sell"]),
            (["Code", "Buy", "Sell"], "code_list", "$", ["Code", "Buy", "Sell"]),
            (
                ["$Code", "$Buy", "Sell"],
                "code_list",
                "$$",
                ["$Code", "$Buy", "Sell"],
            ),
        ],
    )
    def test_filter_list_using_first_character(
        self, value, value_name, first_character, expected_outcome
    ):

        updated_list = Validator(value, value_name).filter_list_using_first_character(
            first_character
        )

        assert isinstance(updated_list, Validator)

        assert updated_list.value == expected_outcome

    @pytest.mark.parametrize(
        "value, value_name, superset, superset_name",
        [
            (
                ["Portfolio", "Transaction"],
                "file_types",
                ["Portfolio", "Transaction", "Holding"],
                "all_file_types",
            ),
        ],
    )
    def test_check_subset_of_list_success(
        self, value, value_name, superset, superset_name
    ):

        Validator(value, value_name).check_subset_of_list(superset, superset_name)

    @pytest.mark.parametrize(
        "value, value_name, superset, superset_name, expected_exception",
        [
            (
                ["Portfolio", "Transaction", "Quote"],
                "file_types",
                ["Portfolio", "Transaction", "Holding"],
                "all_file_types",
                ValueError,
            ),
        ],
    )
    def test_check_subset_of_list_exception(
        self, value, value_name, superset, superset_name, expected_exception
    ):

        with pytest.raises(expected_exception):
            Validator(value, value_name).check_subset_of_list(superset, superset_name)

    @pytest.mark.parametrize(
        "value, value_name, other_list, list_name",
        [
            (
                ["Portfolio", "Transaction", "Quote"],
                "file_types",
                ["Instrument", "Holding"],
                "other_file_types",
            ),
        ],
    )
    def test_check_no_intersection_with_list_success(
        self, value, value_name, other_list, list_name
    ):

        Validator(value, value_name).check_no_intersection_with_list(
            other_list, list_name
        )

    @pytest.mark.parametrize(
        "value, value_name, other_list, list_name, expected_exception",
        [
            (
                ["Portfolio", "Transaction", "Quote"],
                "file_types",
                ["Instrument", "Holding", "Quote"],
                "other_file_types",
                ValueError,
            ),
        ],
    )
    def test_check_no_intersection_with_list_exception(
        self, value, value_name, other_list, list_name, expected_exception
    ):

        with pytest.raises(expected_exception):
            Validator(value, value_name).check_no_intersection_with_list(
                other_list, list_name
            )

    @pytest.mark.parametrize(
        "value, expected_message_part1, expected_message_part2",
        [
            (
                [{"foo": "bar"}],
                "The value [{'foo': 'bar'}] provided in property_columns is invalid.",
                "{'foo': 'bar'} does not contain the mandatory 'source' key.",
            ),
            (
                [{"source": 2}],
                "The value [{'source': 2}] provided in property_columns is invalid.",
                "2 in {'source': 2} is not a string.",
            ),
            (
                [1],
                "The value [1] provided in property_columns is invalid",
                "1 is not a string or dictionary.",
            ),
            (
                [1, {"foo": "bar"}, {"source": [5]}],
                "The value [1, {'foo': 'bar'}, {'source': [5]}] provided in property_columns is invalid",
                "1 is not a string or dictionary, "
                + "{'foo': 'bar'} does not contain the mandatory 'source' key, "
                + "[5] in {'source': [5]} is not a string.",
            ),
        ],
    )
    def test_check_entries_are_strings_or_dict_containing_key_invalid_values(
        self, value, expected_message_part1, expected_message_part2
    ):
        with pytest.raises(ValueError) as exc_info:
            Validator(
                value, "property_columns"
            ).check_entries_are_strings_or_dict_containing_key("source")

        assert expected_message_part1 in str(exc_info.value), str(exc_info.value)
        assert expected_message_part2 in str(exc_info.value), str(exc_info.value)

    @pytest.mark.parametrize(
        "value",
        [
            [{"source": "foo"}],
            ["foo"],
            ["foo", {"source": "bar"}],
        ],
    )
    def test_check_entries_are_strings_or_dict_containing_key_valid_values(
        self, value
    ):
        Validator(
            value, "property_columns"
        ).check_entries_are_strings_or_dict_containing_key("source")
