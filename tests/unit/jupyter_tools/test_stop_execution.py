import pytest
from finbourne_sdk_utils.jupyter_tools import StopExecution


class TestPortfolioStopper:
    def test_portfolio_stopper(self) -> None:

        portfolio_code = "testPortfolio"
        error_message = f"Portfolio {portfolio_code} does not exist. Stopping notebook"

        with pytest.raises(StopExecution) as exc_info:
            raise StopExecution(error_message)

        assert exc_info.value.message == error_message
