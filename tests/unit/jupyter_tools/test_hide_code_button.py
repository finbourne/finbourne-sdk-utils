import unittest
from finbourne_sdk_utils.jupyter_tools import toggle_code


class TestHideCodeButton(unittest.TestCase):
    def test_portfolio_stopper(self) -> None:

        test_toggle = toggle_code("Toggle Docstring")
        self.assertIsNone(test_toggle)
