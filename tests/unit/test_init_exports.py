import unittest


class TestTopLevelInitExports(unittest.TestCase):
    """Test that the top-level __init__.py exports all subpackages."""

    def test_import_cocoon_subpackage(self):
        import finbourne_sdk_utils.cocoon
        self.assertTrue(hasattr(finbourne_sdk_utils.cocoon, "cocoon"))

    def test_import_extract_subpackage(self):
        import finbourne_sdk_utils.extract
        self.assertTrue(hasattr(finbourne_sdk_utils.extract, "get_holdings_for_group"))

    def test_import_jupyter_tools_subpackage(self):
        import finbourne_sdk_utils.jupyter_tools
        self.assertTrue(hasattr(finbourne_sdk_utils.jupyter_tools, "StopExecution"))
        self.assertTrue(hasattr(finbourne_sdk_utils.jupyter_tools, "toggle_code"))

    def test_import_logger_subpackage(self):
        import finbourne_sdk_utils.logger
        self.assertTrue(hasattr(finbourne_sdk_utils.logger, "LusidLogger"))

    def test_import_pandas_utils_subpackage(self):
        import finbourne_sdk_utils.pandas_utils
        self.assertTrue(hasattr(finbourne_sdk_utils.pandas_utils, "lusid_response_to_data_frame"))


class TestCocoonInitExports(unittest.TestCase):
    """Test that the cocoon __init__.py exports key functions and modules."""

    def test_export_load_from_data_frame(self):
        from finbourne_sdk_utils.cocoon import load_from_data_frame
        self.assertTrue(callable(load_from_data_frame))

    def test_export_resolve_instruments(self):
        from finbourne_sdk_utils.cocoon import resolve_instruments
        self.assertTrue(callable(resolve_instruments))

    def test_export_create_property_values(self):
        from finbourne_sdk_utils.cocoon import create_property_values
        self.assertTrue(callable(create_property_values))

    def test_export_set_attributes_recursive(self):
        from finbourne_sdk_utils.cocoon import set_attributes_recursive
        self.assertTrue(callable(set_attributes_recursive))

    def test_export_seed_data(self):
        from finbourne_sdk_utils.cocoon import seed_data
        self.assertTrue(callable(seed_data))

    def test_export_utility_functions(self):
        from finbourne_sdk_utils.cocoon import (
            checkargs,
            load_data_to_df_and_detect_delimiter,
            check_mapping_fields_exist,
            parse_args,
            identify_cash_items,
            validate_mapping_file_structure,
            get_delimiter,
            scale_quote_of_type,
            strip_whitespace,
            load_json_file,
            default_fx_forward_model,
        )
        for func in [
            checkargs,
            load_data_to_df_and_detect_delimiter,
            check_mapping_fields_exist,
            parse_args,
            identify_cash_items,
            validate_mapping_file_structure,
            get_delimiter,
            scale_quote_of_type,
            strip_whitespace,
            load_json_file,
            default_fx_forward_model,
        ]:
            self.assertTrue(callable(func))

    def test_export_printer_functions(self):
        from finbourne_sdk_utils.cocoon import (
            format_holdings_response,
            format_instruments_response,
            format_portfolios_response,
            format_quotes_response,
            format_transactions_response,
        )
        for func in [
            format_holdings_response,
            format_instruments_response,
            format_portfolios_response,
            format_quotes_response,
            format_transactions_response,
        ]:
            self.assertTrue(callable(func))

    def test_export_async_tools_module(self):
        import finbourne_sdk_utils.cocoon.async_tools
        self.assertIsNotNone(finbourne_sdk_utils.cocoon.async_tools)

    def test_export_validator_module(self):
        import finbourne_sdk_utils.cocoon.validator
        self.assertIsNotNone(finbourne_sdk_utils.cocoon.validator)

    def test_export_dateorcutlabel_module(self):
        import finbourne_sdk_utils.cocoon.dateorcutlabel
        self.assertIsNotNone(finbourne_sdk_utils.cocoon.dateorcutlabel)


class TestExtractInitExports(unittest.TestCase):
    """Test that the extract __init__.py exports correctly."""

    def test_export_get_holdings_for_group(self):
        from finbourne_sdk_utils.extract import get_holdings_for_group
        self.assertTrue(callable(get_holdings_for_group))


class TestJupyterToolsInitExports(unittest.TestCase):
    """Test that the jupyter_tools __init__.py exports correctly."""

    def test_export_stop_execution(self):
        from finbourne_sdk_utils.jupyter_tools import StopExecution
        self.assertTrue(isinstance(StopExecution, type))

    def test_export_toggle_code(self):
        from finbourne_sdk_utils.jupyter_tools import toggle_code
        self.assertTrue(callable(toggle_code))


class TestLoggerInitExports(unittest.TestCase):
    """Test that the logger __init__.py exports correctly."""

    def test_export_lusid_logger(self):
        from finbourne_sdk_utils.logger import LusidLogger
        self.assertTrue(isinstance(LusidLogger, type))


class TestPandasUtilsInitExports(unittest.TestCase):
    """Test that the pandas_utils __init__.py exports correctly."""

    def test_export_lusid_response_to_data_frame(self):
        from finbourne_sdk_utils.pandas_utils import lusid_response_to_data_frame
        self.assertTrue(callable(lusid_response_to_data_frame))
