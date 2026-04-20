import pandas
import pytest

from finbourne_sdk_utils.lpt import lpt

EMPTY_DF = pandas.DataFrame()


class TestReadInputForExcel:
    """
    Tests that read_input can support both windows and unix specific file paths. In either case need to ensure that
    paths both with and without a suffixed sheet name are correctly parsed and then read as excel files into a
    dataframe.
    """

    @pytest.mark.parametrize(
        "input_path, expected_path, expected_sheet_name",
        [
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsx:Multiple",
                "C:\\finbourne\\holdings-examples.xlsx",
                "Multiple",
                id="Windows absolute path with a sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsx:Multiple",
                "/home/finbourne/holdings-examples.xlsx",
                "Multiple",
                id="Unix absolute path with a sheet",
            ),
            pytest.param(
                "C:\\holdings-examples.xlsx:Multiple",
                "C:\\holdings-examples.xlsx",
                "Multiple",
                id="Windows absolute path at root with a sheet",
            ),
            pytest.param(
                "/holdings-examples.xlsx:Multiple",
                "/holdings-examples.xlsx",
                "Multiple",
                id="Unix absolute path at root with a sheet",
            ),
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsx",
                "C:\\finbourne\\holdings-examples.xlsx",
                0,
                id="Windows absolute path with no sheet",
            ),
            pytest.param(
                "/holdings-examples.xlsx",
                "/holdings-examples.xlsx",
                0,
                id="Unix absolute path with no sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings:examples.xlsx:Multiple",
                "/home/finbourne/holdings:examples.xlsx",
                "Multiple",
                id="Unix path with a colon in the file name with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings:examples.xlsx",
                "/home/finbourne/holdings:examples.xlsx",
                0,
                id="Unix files with a colon in the file name with no sheet",
            ),
            pytest.param(
                ".\\finbourne\\holdings-examples.xlsx:Multiple",
                ".\\finbourne\\holdings-examples.xlsx",
                "Multiple",
                id="Windows relative path with a sheet",
            ),
            pytest.param(
                "./finbourne/holdings-examples.xlsx:Multiple",
                "./finbourne/holdings-examples.xlsx",
                "Multiple",
                id="Unix relative path with a sheet",
            ),
            pytest.param(
                ".\\finbourne\\holdings-examples.xlsx",
                ".\\finbourne\\holdings-examples.xlsx",
                0,
                id="Windows relative path with no sheet",
            ),
            pytest.param(
                "./finbourne/holdings-examples.xlsx",
                "./finbourne/holdings-examples.xlsx",
                0,
                id="Unix relative path with no sheet",
            ),
        ],
    )
    def test_read_input_for_excel(
        self, input_path, expected_path, expected_sheet_name
    ):
        with pytest.MonkeyPatch.context() as mp:
            calls = []
            mp.setattr("pandas.read_excel", lambda *args, **kwargs: (calls.append((args, kwargs)), EMPTY_DF)[1])
            lpt.read_input(path=input_path)
            assert len(calls) == 1
            assert calls[0] == ((expected_path,), {"sheet_name": expected_sheet_name, "engine": "openpyxl"})


class TestReadInputForNonExcel:
    """
    Tests that non excel file paths do not attempt to retrieve a sheet (which should not be
    viable) and are instead read as csv files into a dataframe.
    """

    @pytest.mark.parametrize(
        "input_path",
        [
            pytest.param("C:\\finbourne\\holdings-examples.csv", id="Windows absolute path"),
            pytest.param("/home/finbourne/holdings-examples.csv", id="Unix absolute path"),
            pytest.param("C:\\holdings-examples.txt", id="Windows absolute path at root"),
            pytest.param("/holdings-examples.txt", id="Unix absolute path at root"),
            pytest.param(".\\finbourne\\holdings-examples.csv", id="Windows relative path"),
            pytest.param("./finbourne/holdings-examples.csv", id="Unix relative path"),
            # unlikely or erroneous case of non excel file with sheet
            pytest.param(
                "C:\\finbourne\\holdings-examples.xyz:ShouldNotHappen",
                id="Windows non excel file with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xyz:ShouldNotHappen",
                id="Unix non excel file with sheet",
            ),
        ],
    )
    def test_read_input_for_non_excel(self, input_path):
        with pytest.MonkeyPatch.context() as mp:
            calls = []
            mp.setattr("pandas.read_csv", lambda *args, **kwargs: (calls.append((args, kwargs)), EMPTY_DF)[1])
            lpt.read_input(path=input_path)
            assert len(calls) == 1
            assert calls[0] == ((input_path,), {})


class TestIsPathSupportedExcelWithSheet:
    """
    Tests that all supported excel inputs (xls, xlsx, xlsb, xlsm) with sheets suffixed to the path are matched on
    both unix and windows platforms.
    """

    @pytest.mark.parametrize(
        "input_path",
        [
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsx:Multiple",
                id="Windows absolute path with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsx:Multiple",
                id="Unix absolute path with sheet",
            ),
            pytest.param(
                "C:\\holdings-examples.xlsx:Multiple",
                id="Windows absolute path at root with sheet",
            ),
            pytest.param(
                "/holdings-examples.xlsx:Multiple",
                id="Unix absolute path at root with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings:examples.xlsx:Multiple",
                id="Unix allows files with colon in name with sheet",
            ),
            pytest.param(
                "C:\\finbourne\\holdings-examples.xls:Multiple",
                id="Windows path for xls with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xls:Multiple",
                id="Unix path for xls with sheet",
            ),
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsm:Multiple",
                id="Windows path for xlsm with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsm:Multiple",
                id="Unix path for xlsm with sheet",
            ),
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsb:Multiple",
                id="Windows path for xlsb with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsb:Multiple",
                id="Unix path for xlsb with sheet",
            ),
        ],
    )
    def test_success(self, input_path):
        assert lpt.is_path_supported_excel_with_sheet(input_path), (
            input_path + " should be a valid excel path with a sheet."
        )

    @pytest.mark.parametrize(
        "input_path",
        [
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsx",
                id="Windows absolute path with no sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsx",
                id="Unix absolute path with no sheet",
            ),
            pytest.param("C:\\holdings-examples.xlsx", id="Windows absolute path at root"),
            pytest.param("/holdings-examples.xlsx", id="Unix absolute path with no sheet at root"),
            pytest.param(
                "/home/finbourne/holdings:examples.xlsx",
                id="Unix allows files with colon in name with no sheet",
            ),
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsm",
                id="Windows path for xlsm with no sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsm",
                id="Unix path for xlsm with no sheet",
            ),
            pytest.param(
                "C:\\finbourne\\holdings-examples.xlsb",
                id="Windows path for xlsb with no sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xlsb",
                id="Unix path for xlsb with no sheet",
            ),
            pytest.param("/home/finbourne/holdings-examples.xyz", id="Non excel file"),
            # unlikely or erroneous case of non excel file with sheet
            pytest.param(
                "C:\\finbourne\\holdings-examples.xyz:ShouldNotHappen",
                id="Windows non excel file with sheet",
            ),
            pytest.param(
                "/home/finbourne/holdings-examples.xyz:ShouldNotHappen",
                id="Unix non excel file with sheet",
            ),
        ],
    )
    def test_failure(self, input_path):
        assert not lpt.is_path_supported_excel_with_sheet(input_path), (
            input_path + " should not be a valid excel path with a sheet."
        )
