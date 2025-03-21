import os

import lusid
import lusid.models as models
import unittest


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
from parameterized import parameterized

instrument_id = "LUID_01234567"


instrument_success = models.UpsertInstrumentsResponse(
    values={
        "LUID_01234567": models.Instrument(
            lusid_instrument_id="LUID_01234567",
            version= models.Version(as_at_version_number=1,effective_from="2010-09-01T09:31:22.664000+00:00",asAtDate="2019-09-01T09:31:22.664000+00:00"),
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
    version= models.Version(as_at_version_number=1,effective_from="2010-09-01T09:31:22.664000+00:00",asAtDate="2019-09-01T09:31:22.664000+00:00"),
    type="Transaction",
    display_name="name2",
    description="test portfolio",
    created="2010-09-01T09:31:22.664000+00:00",
    href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    id=models.ResourceId(code="ID00001", scope="default test"),
)

transaction_success = models.UpsertPortfolioTransactionsResponse(
    href="https://www.notadonaim.lusid.com/api/api/code/transactions?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    links=[],
    version= models.Version(as_at_version_number=1,effective_from="2010-09-01T09:31:22.664000+00:00",asAtDate="2019-09-01T09:31:22.664000+00:00"),
)

quote_success = models.UpsertQuotesResponse(
    failed={
        "BBG001MM1KV4-Figi_2019-10-28": models.ErrorDetail(
            id="BBG001MM1KV4",
            type="Price",
            details="Quote had an error",
        )
    },
    values={
        "BBG001MM1KV4-Figi_2019-10-28": models.Quote(
            as_at="2019-01-16T09:31:22.664000+00:00",
            quote_id=models.QuoteId(
                quote_series_id=models.QuoteSeriesId(
                    provider="Default",
                    instrument_id="BBG001MM1KV4",
                    instrument_id_type="Figi",
                    quote_type="Price",
                    field="Mid",
                ),
                effective_at="2019-01-16T09:31:22.664000+00:00",
            ),
            uploaded_by="test",
        )
    },
)

adjust_holding_success = models.AdjustHolding(
    href="https://notadomain.lusid.com/api/api/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    version=models.Version(as_at_version_number=1,effective_from="2010-09-01T09:31:22.664000+00:00",asAtDate="2019-09-01T09:31:22.664000+00:00"),
)

reference_portfolio_success = models.Portfolio(
    links=[],
    version=models.Version(as_at_version_number=1,effective_from="2010-09-01T09:31:22.664000+00:00",asAtDate="2019-09-01T09:31:22.664000+00:00"),
    type="Transaction",
    display_name="name3",
    description="test reference portfolio",
    created="2016-01-01T09:31:22.664000+00:00",
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


api_exception = lusid.exceptions.ApiException(http_resp=MockHttpResp())


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


class CocoonPrinterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
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
            self.assertEqual(num_items, len(succ))
            for index, row in succ.iterrows():
                self.assertEqual(expected_value["succ"][index], row[succ.columns[0]])

        # removed datetime from tests as the json flatten has unexpected behaviour for datetime, seems to use a Timestamp(str)
        # datetimes not incluced in the V1 test coverage
        # also appears to use the alias varient of the property 
        if succ is not None and data_entity_details:
            self.assertEqual(num_items, len(succ))
            for index, row in succ.iterrows():
                self.assertEqual(
                    expected_value["succ"][index],
                    {k: v for (k, v) in row.to_dict().items() if v and ("asAtDate" in k) == False and ("effectiveFrom" in k )== False }, 
                )

        if err is not None and not err_extended:
            self.assertEqual(num_items, len(err))
            for index, row in err.iterrows():
                self.assertEqual(expected_value["err"][index], row[err.columns[0]])

        if err is not None and err_extended:
            self.assertEqual(num_items, len(err))
            for index, row in err.iterrows():
                self.assertEqual(expected_value["err"][index][0], row[err.columns[0]])
                self.assertEqual(expected_value["err"][index][1], row[err.columns[2]])

        if failed is not None and not data_entity_details:
            self.assertEqual(num_items, len(failed))
            for index, row in failed.iterrows():
                self.assertEqual(
                    expected_value["failed"][index], row[failed.columns[0]]
                )

        if failed is not None and data_entity_details:
            self.assertEqual(num_items, len(failed))
            for index, row in failed.iterrows():
                self.assertEqual(
                    expected_value["failed"][index],
                    {k: v for (k, v) in row.to_dict().items() if v},
                )

    @parameterized.expand(
        [
            (
                "standard_quotes",
                [
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                ],
                "quotes",
                ["code", "code1", "code12"],
            ),
            (
                "standard_holdings",
                [
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                ],
                "holdings",
                ["code", "code1", "code12"],
            ),
        ]
    )
    def test_get_portfolio_from_href(self, _, href, file_type, expected_value):
        codes = get_portfolio_from_href(href=href, file_type=file_type)
        self.assertEqual(len(href), len(codes))
        self.assertEqual(expected_value, codes)

    @parameterized.expand(
        [
            (
                "standard_response",
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
                "standard_response_fully_expanded",
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
            ("empty_response", empty_response_with_full_shape, 0, {}, False, False),
            (
                "empty_response_missing_shape",
                empty_response_missing_shape,
                0,
                {},
                False,
                False,
            ),
            (
                "standard_response_with_extended_errors",
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
        ]
    )
    def test_format_instruments_response_success(
        self,
        _,
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

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["ID00001", "ID00001"], "err": ["not found", "not found"]},
                False,
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}, False),
            (
                "standard_response_with_extended_errors",
                responses,
                2,
                {"succ": ["ID00001", "ID00001"], "err": extended_error_expected},
                True,
            ),
        ]
    )
    def test_format_portfolios_response_success(
        self,
        _,
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

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["code", "code"], "err": ["not found", "not found"]},
                False,
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}, False),
            (
                "standard_response_with_extended_errors",
                responses,
                2,
                {"succ": ["code", "code"], "err": extended_error_expected},
                True,
            ),
        ]
    )
    def test_format_transactions_response_success(
        self, _, response, num_items, expected_value, extended_errors
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

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {
                    "succ": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "failed": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "err": ["not found", "not found"],
                },
                False,
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}, False),
            (
                "empty_response_missing_shape",
                empty_response_missing_shape,
                0,
                {},
                False,
            ),
            (
                "standard_response_with_extended_errors",
                responses,
                2,
                {
                    "succ": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "failed": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "err": extended_error_expected,
                },
                True,
            ),
        ]
    )
    def test_format_quotes_response_success(
        self, _, response, num_items, expected_value, extended_errors
    ):
        succ, err, failed = format_quotes_response(
            response, extended_error_details=extended_errors
        )
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))
        self.assertEqual(num_items, len(failed))
        quote_success_id_selector = "quoteId.quoteSeriesId.instrumentId"
        quote_failed_id_selector = "id"

        for index, row in succ.iterrows():
            self.assertEqual(
                expected_value["succ"][index],
                row[quote_success_id_selector],
            )
        self.assert_responses(
            num_items, expected_value, err=err, err_extended=extended_errors
        )
        for index, row in failed.iterrows():
            self.assertEqual(
                expected_value["failed"][index],
                row[quote_failed_id_selector],
            )

        printer = self.create_printer(response, "quotes", extended_errors)

        succ, err, failed = printer.format_response()
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))
        self.assertEqual(num_items, len(failed))

        for index, row in succ.iterrows():
            self.assertEqual(
                expected_value["succ"][index],
                row[quote_success_id_selector],
            )
        self.assert_responses(
            num_items, expected_value, err=err, err_extended=extended_errors
        )
        for index, row in failed.iterrows():
            self.assertEqual(
                expected_value["failed"][index],
                row[quote_failed_id_selector],
            )

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["code", "code"], "err": ["not found", "not found"]},
                False,
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}, False),
            (
                "standard_response_with_extended_errors",
                responses,
                2,
                {"succ": ["code", "code"], "err": extended_error_expected},
                True,
            ),
        ]
    )
    def test_format_holdings_response_success(
        self, _, response, num_items, expected_value, extended_errors
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

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["ID00002", "ID00002"], "err": ["not found", "not found"]},
                False,
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}, False),
            (
                "standard_response_with_extended_errors",
                responses,
                2,
                {"succ": ["ID00002", "ID00002"], "err": extended_error_expected},
                True,
            ),
        ]
    )
    def test_format_reference_portfolios_response_success(
        self, _, response, num_items, expected_value, extended_errors
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

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_instruments_response_fail(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_instruments_response(response)

        with self.assertRaises(expected_error):
            printer = self.create_printer(response, "instruments")
            succ, err, failed = printer.format_response()

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_portfolios_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_portfolios_response(response)

        with self.assertRaises(expected_error):
            printer = self.create_printer(response, "portfolios")
            succ, err, failed = printer.format_response()

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_transactions_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_transactions_response(response)

        with self.assertRaises(expected_error):
            printer = self.create_printer(response, "transactions")
            succ, err, failed = printer.format_response()

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_quotes_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_quotes_response(response)

        with self.assertRaises(expected_error):
            printer = self.create_printer(response, "quotes")
            succ, err, failed = printer.format_response()

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_holdings_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_holdings_response(response)

        with self.assertRaises(expected_error):
            printer = self.create_printer(response, "holdings")
            succ, err, failed = printer.format_response()

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_reference_portfolios_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_reference_portfolios_response(response)

        with self.assertRaises(expected_error):
            printer = self.create_printer(response, "reference_portfolios")
            succ, err, failed = printer.format_response()

    def test_invalid_key(self):
        with self.assertRaises(ValueError) as context:
            CocoonPrinter({"foo": {}})

        self.assertTrue("Response contains invalid key" in str(context.exception))

    def test_no_keys(self):
        with self.assertRaises(ValueError) as context:
            CocoonPrinter({})

        self.assertTrue("Response doesn't contain any keys." in str(context.exception))

    def test_too_many_keys(self):
        with self.assertRaises(ValueError) as context:
            CocoonPrinter(
                {
                    "instruments": {},
                    "portfolios": {},
                }
            )

        self.assertEqual(
            "Response contains too many keys - only one is allowed, but received ['instruments', 'portfolios']",
            str(context.exception),
        )

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
