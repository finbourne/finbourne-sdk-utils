from finbourne_sdk_utils.jupyter_tools import toggle_code


class TestHideCodeButton:
    def test_portfolio_stopper(self) -> None:

        test_toggle = toggle_code("Toggle Docstring")
        assert test_toggle is None
