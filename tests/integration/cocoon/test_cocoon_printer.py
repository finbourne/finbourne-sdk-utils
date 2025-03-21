import os
import re
import unittest
import json
from pathlib import Path
import pandas as pd
import lusid
from finbourne_sdk_utils import logger
from finbourne_sdk_utils.cocoon.cocoon import load_from_data_frame
from finbourne_sdk_utils.cocoon.cocoon_printer import format_portfolios_response


class CocoonPrinterIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        
        cls.api_factory = lusid.SyncApiClientFactory()
        
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    def test_format_portfolios_response_includes_extended_errors(self):
        """
        Test that the cocoon printer also returns the RequestID and ErrorDetails for any errors in the response.
        """
        # Load a portfolio that will contain an error using load_from_data_frame
        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath("data/cocoon_format_portfolio_errors.csv")
        )
        portfolio_response = load_from_data_frame(
            api_factory=self.api_factory,
            scope="cocoon_format_errors",
            data_frame=data_frame,
            mapping_required={
                "code": "FundCode",
                "display_name": "display_name",
                "created": "created",
                "base_currency": "base_currency",
            },
            mapping_optional={"description": "description", "accounting_method": None},
            file_type="portfolios",
        )

        # Pass the response into the cocoon printer formatter
        succ, err = format_portfolios_response(
            portfolio_response, extended_error_details=True
        )

        # Assert that the error part includes RequestID details and ErrorDetails
        self.assertEqual(2, len(succ))
        self.assertEqual(1, len(err))
        for index, row in err.iterrows():
            self.assertEqual(row[err.columns[0]], "Bad Request")
            
            
            # regex for new and old request id patterns
            patterns = [r"([A-Z0-9]+:[A-Z0-9]+)", r"(\d{10}-[a-f0-9]{32})"]

            foundRequestId = False
            for pattern in patterns:
                match = re.search(pattern, row[err.columns[2]])
                if match:
                    print(f"{match.group(1)} matched pattern: {pattern}")
                    foundRequestId = True
                    break
                
            self.assertTrue(foundRequestId, "No request id found in error message")
                    
            # Deserialise the ErrorDetails field and check one of the values
            self.assertEqual(
                json.loads(row[err.columns[3]]).get("name"), "UndefinedCurrencyFailure"
            )

        # Delete the portfolios at the end of the test
        for portfolio in portfolio_response.get("portfolios").get("success"):
            self.api_factory.build(lusid.api.PortfoliosApi).delete_portfolio(
                scope="cocoon_format_errors", code=portfolio.id.code
            )


if __name__ == "__main__":
    unittest.main()
