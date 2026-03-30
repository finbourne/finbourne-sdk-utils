from http import HTTPStatus

import finbourne.sdk.services.lusid as lusid
from finbourne.sdk.extensions import SyncApiClientFactory
from finbourne.sdk.exceptions import ApiException


class MockApiFactory(SyncApiClientFactory):
    """
    This is a mock of the lusid.SyncApiClientFactory class
    """

    def __init__(self):
        pass  # Skip parent __init__ which requires OAuth credentials

    def build(self, api):  # type: ignore[override]
        """
        Creates and returns appropriate Mock for api passed in
        supports:
         - lusid.PropertyDefinitionsApi
        :param lusid.api api: The api to mock

        :return: mock(lusid.api): The mocked api
        """

        if api == lusid.PropertyDefinitionsApi:
            return self.MockPropertyDefinitionsApi()

    class MockPropertyDefinitionsApi:
        """
        A mock of the lusid.PropertyDefinitionsApi
        """

        def create_property_definition(
            self, create_property_definition_request
        ) -> lusid.PropertyDefinition:
            """
            This mocks the creation of a portfolio definition

            :param lusid.CreatePropertyDefinitionRequest create_property_definition_request: The create property definition request
            :param kwargs:
            :return: lusid.PropertyDefinition: The property defintion of the created property
            """
            return lusid.PropertyDefinition(
                key=f"{create_property_definition_request.domain}/{create_property_definition_request.scope}/{create_property_definition_request.code}",
                data_type_id=lusid.ResourceId(
                    scope=create_property_definition_request.data_type_id.scope,
                    code=create_property_definition_request.data_type_id.code,
                ),
            )

        def get_property_definition(
            self, domain, scope, code
        ) -> lusid.PropertyDefinition:
            """
            This mocks the call to get a property definition

            :param domain: The domain of the property
            :param scope: The scope of the property
            :param code: The code of the property
            :param kwargs:

            :return: lusid.PropertyDefinition any: The property definition of the property if it exists
            """
            # Construct the property key
            property_key = f"{domain}/{scope}/{code}"

            # A static representation of the property definitions that exist
            property_keys_in_existance = {
                "Instrument/default/Figi": lusid.ResourceId(
                    scope="system", code="string"
                ),
                "Transaction/default/TradeToPortfolioRate": lusid.ResourceId(
                    scope="system", code="number"
                ),
                "Transaction/Operations/Strategy": lusid.ResourceId(
                    scope="system", code="string"
                ),
                "Holding/Operations/Currency": lusid.ResourceId(
                    scope="system", code="currency"
                ),
            }

            # If the property exists return the defintion, else raise an exception
            if property_key in list(property_keys_in_existance.keys()):
                return lusid.PropertyDefinition(
                    key=property_key,
                    data_type_id=property_keys_in_existance[property_key],
                )
            else:
                raise ApiException(status=HTTPStatus.NOT_FOUND)
