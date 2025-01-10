import os
import unittest
import pandas as pd
from finbourne_sdk_utils import cocoon as cocoon
import lusid

class CocoonTestsSimpleInstruments(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        cls.api_factory = lusid.SyncApiClientFactory()

    def test_load_simple_instrument_properties(self,):
         
        data_frame = pd.DataFrame(
            {
                "Security" : [
                    "ALPHABET INC CAP STK USD0.001 CL C",
                    "ALPHABET INC CAPITAL STOCK USD0.001 CL A",
                    "WALT DISNEY CO",
                    "HOME DEPOT INC., COMMON STOCK, $.05 PAR",
                    "MC DONALDS CORP. COMMON STOCK, NO PAR",
                    "NIKE INC CL B"
                ],
                "Ticker"	: [
                    "GOOG",
                    "GOOGL",
                    "DIS",
                    "HD",
                    "MCD",
                    "NKE"
                ],
                "Asset Type" : [
                    "Equity",
                    "Equity",
                    "Equity",
                    "Equity",
                    "Equity",     
                    "Equity",     
                ],
                "Mkt Price" : [
                    150.01,
                    148.66,
                    88.4,
                    370.87,
                    291.27,
                    78.09            
                ],
                "Cusip" : [
                    "02079K107",
                    "02079K305",
                    "254687106",
                    "437076102",
                    "580135101",
                    "654106103"           
                ],
                "Sector" : [
                    "Communication Services",
                    "Communication Services",
                    "Communication Services",
                    "Consumer Discretionary",
                    "Consumer Discretionary",
                    "Consumer Discretionary"            
                ],
                "Yield" : [
                    0.0053,
                    0.0054,
                    0.0085,
                    0.0243,
                    0.0229,
                    0.019           
                ],
                "Level 1" : [
                    "Risk Assets",
                    "Risk Assets",
                    "Risk Assets",
                    "Risk Assets",
                    "Risk Assets",
                    "Risk Assets"
                ],
                "Level 2" : [
                    "Public Equity",
                    "Public Equity",
                    "Public Equity",
                    "Public Equity",
                    "Public Equity",
                    "Public Equity"
                ],
                "Level 3" : [
                    "US Equity",
                    "US Equity",
                    "US Equity",
                    "US Equity",
                    "US Equity",
                    "US Equity"
                ]
            }
        )


        scope = "wealth"
        instrument_mapping = {
            "required": {
                "name": "Security",
                "definition.instrument_type": "$SimpleInstrument",
                "definition.dom_ccy": "$USD",
                "definition.asset_class": "$Equities",
                "definition.simple_instrument_type": "Asset Type"
            },
            "optional": {},
            "identifiers": {
                "ClientInternal": "Ticker",
                "Ticker": "Ticker",
                "Cusip": "Cusip"
            },
            "properties": [
                {"source": "Level 1", "target": "Level 1"},
                {"source": "Level 2", "target": "Level 2"},
                {"source": "Level 3", "target": "Level 3"},
                {"source": "Yield", "target": "Yield"},
                {"source": "Sector", "target": "Sector"}
            ]
        }


        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=instrument_mapping["required"],
            mapping_optional=instrument_mapping["optional"],
            identifier_mapping=instrument_mapping["identifiers"],
            file_type="instruments",
            instrument_scope=scope,
            property_columns=instrument_mapping["properties"]
        )

        self.assertEqual(len(responses["instruments"]["success"]), 1)
        self.assertEqual(len(responses["instruments"]["errors"]), 0)
     
