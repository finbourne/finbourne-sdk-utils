import os

import finbourne.sdk.services.lusid.models as models
from finbourne.sdk.exceptions import ApiException
from datetime import datetime
import pytest

import pytz


from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.cocoon_printer import (
    format_portfolios_response,
    format_instruments_response,
    format_holdings_response,
    format_quotes_response,
    format_transactions_response,
    get_portfolio_from_href,
    format_reference_portfolios_response,
    CocoonPrinter,
)

instrument_id = "LUID_01234567"
instrument_success = models.UpsertInstrumentsResponse(
    values={
        "LUID_01234567": models.Instrument(
            lusidInstrumentId="LUID_01234567",
            version= models.Version(asAtVersionNumber=1,effectiveFrom=datetime(2010, 9, 1, 9, 31, 22, 664000, pytz.UTC),asAtDate=datetime(2019, 9, 1, 9, 31, 22, 664000, pytz.UTC)),
            name="name1",
            identifiers={
                "Luid": "LUID_01234567",
                "Figi": "BBG000BLNNH6"
            },
            state="Active",
        )
    },
    failed={
        "error101": models.ErrorDetail(
            id="LUID_01234567",
            type="error",
            detail="failed at line 42 in file root.trunk.branch.py",
        )
    },
)

portfolio_success = models.Portfolio(
    links=[],
    version= models.Version(asAtVersionNumber=1,effectiveFrom=datetime(2010, 9, 1, 9, 31, 22, 664000, pytz.UTC),asAtDate=datetime(2019, 9, 1, 9, 31, 22, 664000, pytz.UTC)),
    type="Transaction",
    displayName="name2",
    description="test portfolio",
    created=datetime(2010, 9, 1, 9, 31, 22, 664000, pytz.UTC),
    href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    id=models.ResourceId(code="ID00001", scope="default test"),
)

transaction_success = models.UpsertPortfolioTransactionsResponse(
    href="https://www.notadonaim.lusid.com/api/api/code/transactions?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    links=[],
    version= models.Version(asAtVersionNumber=1,effectiveFrom=datetime(2010, 9, 1, 9, 31, 22, 664000, pytz.UTC),asAtDate=datetime(2019, 9, 1, 9, 31, 22, 664000, pytz.UTC)),
)

quote_success = models.UpsertQuotesResponse(
    failed={
        "BBG001MM1KV4-Figi_2019-10-28": models.ErrorDetail(
            id="BBG001MM1KV4",
            type="Price",
            detail="Quote had an error",
        )
    },
    values={
        "BBG001MM1KV4-Figi_2019-10-28": models.Quote(
            asAt=datetime(2019, 1, 16, 9, 31, 22, 664000, pytz.UTC),
            quoteId=models.QuoteId(
                quoteSeriesId=models.QuoteSeriesId(
                    provider="Default",
                    instrumentId="BBG001MM1KV4",
                    instrumentIdType="Figi",
                    quoteType="Price",
                    field="Mid",
                ),
                effectiveAt="2019-01-16T09:31:22.664000+00:00",
            ),
            uploadedBy="test",
        )
    },
)

adjust_holding_success = models.AdjustHolding(
    href="https://notadomain.lusid.com/api/api/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    version=models.Version(asAtVersionNumber=1,effectiveFrom=datetime(2010, 9, 1, 9, 31, 22, 664000, pytz.UTC),asAtDate=datetime(2019, 9, 1, 9, 31, 22, 664000, pytz.UTC)),
)

reference_portfolio_success = models.Portfolio(
    links=[],
    version=models.Version(asAtVersionNumber=1,effectiveFrom=datetime(2010, 9, 1, 9, 31, 22, 664000, pytz.UTC),asAtDate=datetime(2019, 9, 1, 9, 31, 22, 664000, pytz.UTC)),
    type="Transaction",
    displayName="name3",
    description="test reference portfolio",
    created=datetime(2016, 1, 1, 9, 31, 22, 664000, pytz.UTC),
    href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    id=models.ResourceId(code="ID00002", scope="default test"),
)


class MockHttpResp:
    status = "404"
    reason = "not found"
    data = b'{"name":"TestFailure","errorDetails":[],"code":404,"type":"https://docs.lusid.com/#section/Error-Codes/404","title":"This is a test error title","status":404,"detail":"Test detail","instance":"https://test.lusid.com/app/insights/logs/TestRequestID-0001","extensions":{}}'

    @classmethod
    def getheaders(cls):
        return {"lusid-meta-requestId": "TestRequestID-0001"}


api_exception = ApiException(http_resp=MockHttpResp())


# build finbourne_sdk_utils responses

responses = {
    "instruments": {
        "errors": [api_exception for _ in range(2)],
        "success": [instrument_success for _ in range(2)],
    },
    "portfolios": {
        "errors": [api_exception for _ in range(2)],
        "success": [portfolio_success for _ in range(2)],
    },
    "transactions": {
        "errors": [api_exception for _ in range(2)],
        "success": [transaction_success for _ in range(2)],
    },
    "quotes": {
        "errors": [api_exception for _ in range(2)],
        "success": [quote_success for _ in range(2)],
    },
    "holdings": {
        "errors": [api_exception for _ in range(2)],
        "success": [adjust_holding_success for _ in range(2)],
    },
    "reference_portfolios": {
        "errors": [api_exception for _ in range(2)],
        "success": [reference_portfolio_success for _ in range(2)],
    },
}

empty_response_with_full_shape = {
    "instruments": {
        "errors": [],
        "success": [
            models.UpsertInstrumentsResponse(values={}, failed={}),
        ],
    },
    "portfolios": {
        "errors": [],
        "success": [],
    },
    "transactions": {
        "errors": [],
        "success": [],
    },
    "quotes": {
        "errors": [],
        "success": [models.UpsertQuotesResponse(failed={}, values={})],
    },
    "holdings": {
        "errors": [],
        "success": [],
    },
    "reference_portfolios": {
        "errors": [],
        "success": [],
    },
}

empty_response_missing_shape = {
    "instruments": {
        "errors": [],
        "success": [],
    },
    "portfolios": {
        "errors": [],
        "success": [],
    },
    "transactions": {
        "errors": [],
        "success": [],
    },
    "quotes": {"errors": [], "success": []},
    "holdings": {
        "errors": [],
        "success": [],
    },
    "reference_portfolios": {
        "errors": [],
        "success": [],
    },
}

responses_no_error_field = {
    "instruments": {
        "success": [instrument_success],
    },
    "portfolios": {
        "success": [portfolio_success],
    },
    "transactions": {
        "success": [transaction_success],
    },
    "quotes": {"success": [quote_success]},
    "holdings": {
        "success": [adjust_holding_success],
    },
    "reference_portfolios": {
        "success": [portfolio_success],
    },
}
responses_no_success_field = {
    "instruments": {
        "errors": [api_exception for _ in range(2)],
    },
    "portfolios": {
        "errors": [api_exception for _ in range(2)],
    },
    "transactions": {
        "errors": [api_exception for _ in range(2)],
    },
    "quotes": {
        "errors": [api_exception for _ in range(2)],
    },
    "holdings": {
        "errors": [api_exception for _ in range(2)],
    },
    "reference_portfolios": {
        "errors": [api_exception for _ in range(2)],
    },
}

extended_error_expected = [
    ["not found", "TestRequestID-0001"],
    ["not found", "TestRequestID-0001"],
]


class TestCocoonPrinter:
    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    def assert_responses(
        self,
        num_items,
        expected_value,
        succ=None,
        err=None,
        failed=None,
        err_extended=False,
        data_entity_details=False,
    ):
        if succ is not None and not data_entity_details:
            assert num_items == len(succ)
            for index, row in succ.iterrows():
                assert expected_value["succ"][index] == row[succ.columns[0]]

        # removed datetime from tests as the json flatten has unexpected behaviour for datetime, seems to use a Timestamp(str)
        # datetimes not incluced in the V1 test coverage
        # also appears to use the alias varient of the property 
        if succ is not None and data_entity_details:
            assert num_items == len(succ)
            for index, row in succ.iterrows():
                assert expected_value["succ"][index] == {k: v for (k, v) in row.to_dict().items() if v and ("asAtDate" not in k) and ("effectiveFrom" not in k)}

        if err is not None and not err_extended:
            assert num_items == len(err)
            for index, row in err.iterrows():
                assert expected_value["err"][index] == row[err.columns[0]]

        if err is not None and err_extended:
            assert num_items == len(err)
            for index, row in err.iterrows():
                assert expected_value["err"][index][0] == row[err.columns[0]]
                assert expected_value["err"][index][1] == row[err.columns[2]]

        if failed is not None and not data_entity_details:
            assert num_items == len(failed)
            for index, row in failed.iterrows():
                assert expected_value["failed"][index] == row[failed.columns[0]]

        if failed is not None and data_entity_details:
            assert num_items == len(failed)
            for index, row in failed.iterrows():
                assert expected_value["failed"][index] == {k: v for (k, v) in row.to_dict().items() if v}

    @pytest.mark.parametrize(
        "href, file_type, expected_value",
        [
            (
                [
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                ],
                "quotes",
                ["code", "code1", "code12"],
            ),
            (
                [
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                ],
                "holdings",
                ["code", "code1", "code12"],
            ),
        ],
    )
    def test_get_portfolio_from_href(self, href, file_type, expected_value):
        codes = get_portfolio_from_href(href=href, file_type=file_type)
        assert len(href) == len(codes)
        assert expected_value == codes

    @pytest.mark.parametrize(
        "response, num_items, expected_value, extended_errors, data_entity_details",
        [
            (
                responses,
                2,
                {
                    "succ": [
                        instrument_id,
                        instrument_id,
                    ],
                    "failed": [
                        "error101",
                        "error101",
                    ],
                    "err": ["not found", "not found"],
                },
                False,
                False,
            ),
            (
                responses,
                2,
                {
                    "succ": [
                        {
                            "lusidInstrumentId": "LUID_01234567",
                            "version.asAtVersionNumber":1,
                            "state": "Active",
                            "identifiers.Luid": "LUID_01234567",
                            "identifiers.Figi": "BBG000BLNNH6",
                            "name": "name1",
                        },
                        {
                            "lusidInstrumentId": "LUID_01234567",
                            "version.asAtVersionNumber":1,
                            "state": "Active",
                            "identifiers.Luid": "LUID_01234567",
                            "identifiers.Figi": "BBG000BLNNH6",
                            "name": "name1",
                        },
                    ],
                    "failed": [
                        {
                            "id": "LUID_01234567",
                            "detail": "failed at line 42 in file root.trunk.branch.py",
                            "type": "error",
                        },
                         {
                            "id": "LUID_01234567",
                            "detail": "failed at line 42 in file root.trunk.branch.py",
                            "type": "error",
                        },
                    ],
                    "err": ["not found", "not found"],
                },
                False,
                True,
            ),
            (empty_response_with_full_shape, 0, {}, False, False),
            (
                empty_response_missing_shape,
                0,
                {},
                False,
                False,
            ),
            (
                responses,
                2,
                {
                    "succ": [
                        "LUID_01234567",
                        "LUID_01234567",
                    ],
                    "failed": [
                        "error101",
                        "error101",
                    ],
                    "err": extended_error_expected,
                },
                True,
                False,
            ),
        ],
    )
    def test_format_instruments_response_success(
        self,
        response,
        num_items,
        expected_value,
        extended_errors,
        data_entity_details,
    ):
        succ, err, failed = format_instruments_response(
            response,
            extended_error_details=extended_errors,
            data_entity_details=data_entity_details,
        )
        self.assert_responses(
            num_items,
            expected_value,
            succ=succ,
            err=err,
            failed=failed,
            err_extended=extended_errors,
            data_entity_details=data_entity_details,
        )

        printer = self.create_printer(
            response,
            "instruments",
            extended_errors=extended_errors,
            data_entity_details=data_entity_details,
        )
        succ, err, failed = printer.format_response()
        self.assert_responses(
            num_items,
            expected_value,
            succ=succ,
            err=err,
            failed=failed,
            err_extended=extended_errors,
            data_entity_details=data_entity_details,
        )

    @pytest.mark.parametrize(
        "response, num_items, expected_value, extended_errors",
        [
            (
                responses,
                2,
                {"succ": ["ID00001", "ID00001"], "err": ["not found", "not found"]},
                False,
            ),
            (empty_response_with_full_shape, 0, {}, False),
            (
                responses,
                2,
                {"succ": ["ID00001", "ID00001"], "err": extended_error_expected},
                True,
            ),
        ],
    )
    def test_format_portfolios_response_success(
        self,
        response,
        num_items,
        expected_value,
        extended_errors,
    ):
        succ, err = format_portfolios_response(
            response, extended_error_details=extended_errors
        )
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

        printer = self.create_printer(response, "portfolios", extended_errors)

        succ, err = printer.format_response()
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

    @pytest.mark.parametrize(
        "response, num_items, expected_value, extended_errors",
        [
            (
                responses,
                2,
                {"succ": ["code", "code"], "err": ["not found", "not found"]},
                False,
            ),
            (empty_response_with_full_shape, 0, {}, False),
            (
                responses,
                2,
                {"succ": ["code", "code"], "err": extended_error_expected},
                True,
            ),
        ],
    )
    def test_format_transactions_response_success(
        self, response, num_items, expected_value, extended_errors
    ):
        succ, err = format_transactions_response(
            response, extended_error_details=extended_errors
        )
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

        printer = self.create_printer(response, "transactions", extended_errors)

        succ, err = printer.format_response()
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

    @pytest.mark.parametrize(
        "response, num_items, expected_value, extended_errors",
        [
            (
                responses,
                2,
                {
                    "succ": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "failed": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "err": ["not found", "not found"],
                },
                False,
            ),
            (empty_response_with_full_shape, 0, {}, False),
            (
                empty_response_missing_shape,
                0,
                {},
                False,
            ),
            (
                responses,
                2,
                {
                    "succ": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "failed": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "err": extended_error_expected,
                },
                True,
            ),
        ],
    )
    def test_format_quotes_response_success(
        self, response, num_items, expected_value, extended_errors
    ):
        succ, err, failed = format_quotes_response(
            response, extended_error_details=extended_errors
        )
        assert num_items == len(succ)
        assert num_items == len(err)
        assert num_items == len(failed)
        quote_success_id_selector = "quoteId.quoteSeriesId.instrumentId"
        quote_failed_id_selector = "id"

        for index, row in succ.iterrows():
            assert expected_value["succ"][index] == row[quote_success_id_selector]
        self.assert_responses(
            num_items, expected_value, err=err, err_extended=extended_errors
        )
        for index, row in failed.iterrows():
            assert expected_value["failed"][index] == row[quote_failed_id_selector]

        printer = self.create_printer(response, "quotes", extended_errors)

        succ, err, failed = printer.format_response()
        assert num_items == len(succ)
        assert num_items == len(err)
        assert num_items == len(failed)

        for index, row in succ.iterrows():
            assert expected_value["succ"][index] == row[quote_success_id_selector]
        self.assert_responses(
            num_items, expected_value, err=err, err_extended=extended_errors
        )
        for index, row in failed.iterrows():
            assert expected_value["failed"][index] == row[quote_failed_id_selector]

    @pytest.mark.parametrize(
        "response, num_items, expected_value, extended_errors",
        [
            (
                responses,
                2,
                {"succ": ["code", "code"], "err": ["not found", "not found"]},
                False,
            ),
            (empty_response_with_full_shape, 0, {}, False),
            (
                responses,
                2,
                {"succ": ["code", "code"], "err": extended_error_expected},
                True,
            ),
        ],
    )
    def test_format_holdings_response_success(
        self, response, num_items, expected_value, extended_errors
    ):
        succ, err = format_holdings_response(
            response, extended_error_details=extended_errors
        )
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

        printer = self.create_printer(response, "holdings", extended_errors)

        succ, err = printer.format_response()
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

    @pytest.mark.parametrize(
        "response, num_items, expected_value, extended_errors",
        [
            (
                responses,
                2,
                {"succ": ["ID00002", "ID00002"], "err": ["not found", "not found"]},
                False,
            ),
            (empty_response_with_full_shape, 0, {}, False),
            (
                responses,
                2,
                {"succ": ["ID00002", "ID00002"], "err": extended_error_expected},
                True,
            ),
        ],
    )
    def test_format_reference_portfolios_response_success(
        self, response, num_items, expected_value, extended_errors
    ):
        succ, err = format_reference_portfolios_response(
            response, extended_error_details=extended_errors
        )
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

        printer = self.create_printer(response, "reference_portfolios", extended_errors)

        succ, err = printer.format_response()
        self.assert_responses(
            num_items, expected_value, succ=succ, err=err, err_extended=extended_errors
        )

    # Test failure cases

    @pytest.mark.parametrize(
        "response, expected_error",
        [
            (responses_no_error_field, ValueError),
            (responses_no_success_field, ValueError),
        ],
    )
    def test_format_instruments_response_fail(self, response, expected_error):
        with pytest.raises(expected_error):
            succ, err, failed = format_instruments_response(response)

        with pytest.raises(expected_error):
            printer = self.create_printer(response, "instruments")
            succ, err, failed = printer.format_response()

    @pytest.mark.parametrize(
        "response, expected_error",
        [
            (responses_no_error_field, ValueError),
            (responses_no_success_field, ValueError),
        ],
    )
    def test_format_portfolios_response_fail_no_error_field(
        self, response, expected_error
    ):
        with pytest.raises(expected_error):
            format_portfolios_response(response)

        with pytest.raises(expected_error):
            printer = self.create_printer(response, "portfolios")
            printer.format_response()

    @pytest.mark.parametrize(
        "response, expected_error",
        [
            (responses_no_error_field, ValueError),
            (responses_no_success_field, ValueError),
        ],
    )
    def test_format_transactions_response_fail_no_error_field(
        self, response, expected_error
    ):
        with pytest.raises(expected_error):
            format_transactions_response(response)

        with pytest.raises(expected_error):
            printer = self.create_printer(response, "transactions")
            printer.format_response()

    @pytest.mark.parametrize(
        "response, expected_error",
        [
            (responses_no_error_field, ValueError),
            (responses_no_success_field, ValueError),
        ],
    )
    def test_format_quotes_response_fail_no_error_field(
        self, response, expected_error
    ):
        with pytest.raises(expected_error):
            format_quotes_response(response)

        with pytest.raises(expected_error):
            printer = self.create_printer(response, "quotes")
            printer.format_response()

    @pytest.mark.parametrize(
        "response, expected_error",
        [
            (responses_no_error_field, ValueError),
            (responses_no_success_field, ValueError),
        ],
    )
    def test_format_holdings_response_fail_no_error_field(
        self, response, expected_error
    ):
        with pytest.raises(expected_error):
            format_holdings_response(response)

        with pytest.raises(expected_error):
            printer = self.create_printer(response, "holdings")
            printer.format_response()

    @pytest.mark.parametrize(
        "response, expected_error",
        [
            (responses_no_error_field, ValueError),
            (responses_no_success_field, ValueError),
        ],
    )
    def test_format_reference_portfolios_response_fail_no_error_field(
        self, response, expected_error
    ):
        with pytest.raises(expected_error):
            format_reference_portfolios_response(response)

        with pytest.raises(expected_error):
            printer = self.create_printer(response, "reference_portfolios")
            printer.format_response()

    def test_invalid_key(self):
        with pytest.raises(ValueError) as exc_info:
            CocoonPrinter({"foo": {}})

        assert "Response contains invalid key" in str(exc_info.value)

    def test_no_keys(self):
        with pytest.raises(ValueError) as exc_info:
            CocoonPrinter({})

        assert "Response doesn't contain any keys." in str(exc_info.value)

    def test_too_many_keys(self):
        with pytest.raises(ValueError) as exc_info:
            CocoonPrinter(
                {
                    "instruments": {},
                    "portfolios": {},
                }
            )

        assert "Response contains too many keys - only one is allowed, but received ['instruments', 'portfolios']" == str(exc_info.value)

    def test_instruments(self):
        printer = CocoonPrinter(
            {
                "instruments": {
                    "errors": [api_exception for _ in range(2)],
                    "success": [instrument_success for _ in range(2)],
                },
            }
        )

        succ, err, failed = printer.format_response()
        self.assert_responses(
            2,
            {
                "succ": [
                    instrument_id,
                    instrument_id,
                ],
                "failed": [
                    "error101",
                    "error101",
                ],
                "err": ["not found", "not found"],
            },
            succ=succ,
            err=err,
            failed=failed,
            err_extended=False,
        )

    def create_printer(
        self, response, entity_type, extended_errors=False, data_entity_details=False
    ):
        return CocoonPrinter(
            self.filter_response(response, entity_type),
            extended_error_details=extended_errors,
            data_entity_details=data_entity_details,
        )

    def filter_response(self, response, entity_type):
        return {k: v for k, v in response.items() if k == entity_type}
