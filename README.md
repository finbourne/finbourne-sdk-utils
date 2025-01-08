![LUSID_by_Finbourne](./resources/Finbourne_Logo_Teal.svg)

# Python tools for LUSID

This SDK package contains a set of utility functions for interacting with [LUSID by FINBOURNE](https://support.lusid.com/). To use it you'll need a LUSID account. [Sign up for free at lusid.com](https://www.lusid.com/app/signup)


![PyPI](https://img.shields.io/pypi/v/finbourne_sdk_utils?color=blue)


## Installation

The PyPi package for lusid-python-tools can installed globally on your machine using the following command:

```sh
$ pip install finbourne-sdk-utils
```

or if you are running as a non privileged user you may prefer to install specifically for your user account:

```sh
$ pip install --user finbourne-sdk-utils
```

## Upgrading

To upgrade finbourne_sdk_utils run one of the commands below 

```sh
$ pip install finbourne-sdk-utils -U
```

or

```sh
$ pip install finbourne-sdk-utils -U --user
```
## Source Code

The source code for this package is available on github ( https://github.com/finbourne/finbourne-sdk-utils). Please refer to this repository for more examples and implementation details.


## Examples of Use
Create a python environment and install the package 
```sh
$ pip install finbourne-sdk-utils
```

### Generate a time backed unique id

main.py
```python 
from finbourne_sdk_utils.cocoon.utilities import generate_time_based_unique_id


def main():
    
    # Using a finbourne-sdk-utils utility function to create a random portfolio code
    # Generates a unique ID based on the current time since epoch
    portfolio_code = generate_time_based_unique_id(None)
    
    print(f"Portfolio Code: {portfolio_code} ")
   

if __name__ == "__main__":
    main()

```

### Upsert Instruments from a csv file

global-fund-combined-instrument-master.csv
``` 
instrument_name,client_internal,currency,isin,figi,couprate,s&p rating,moodys_rating
BP_LondonStockEx_BP,imd_43535553,GBP,GB0007980591,BBG000C05BD1,nan,nan,nan
BurfordCapital_LondonStockEx_BUR,imd_43534356,GBP,GG00B4L84979,BBG000PN88Q7,nan,nan,nan
EKFDiagnostics_LondonStockEx_EKF,imd_34535355,GBP,GB0031509804,BBG000BVNBN3,nan,nan,nan
```


main.py

```python
from pathlib import Path
import pandas as pd
from finbourne_sdk_utils import cocoon as cocoon
import lusid

def load_from_csv():
    
    api_factory = lusid.SyncApiClientFactory()
    
    data_frame = pd.read_csv(Path(__file__).parent.joinpath("global-fund-combined-instrument-master.csv"))

    responses = cocoon.load_from_data_frame(
        api_factory=api_factory,
        scope="TestScope1",
        data_frame=data_frame,
        mapping_required={"name": "instrument_name"},
        mapping_optional={},
        file_type="instruments",
        identifier_mapping={"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
        property_columns=["s&p rating", "moodys_rating", "currency"],
        properties_scope="TestPropertiesScope1",
        instrument_scope= "TestScope1",
    )

    rows_loaded =sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["success"]
                ]
            )
    error_count = sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["errors"]
                ]
    )
    
    print(f"Rows loaded = {rows_loaded} rows expected = {len(data_frame)}")
    print(f"Encounted {error_count} errors")
    

if __name__ == "__main__":
    load_from_csv()
```


## Tips for using V2 of the Lusid Python Tools

Existing code that uses the original LusidTools (lusid-python-tools) library is not compatible with V2 of the SDKs. You can use the following information to help port notebooks or your own code to use finbourne_sdk_utils (finbourne-sdk-utils), which is compatible with V2 SDKs.

## Why upgrade

This package is now based on V2 of the Python SDKs. The latest V2 brings many enhancements and language improvements. V1 of the SDKs is now deprecated, and hence any new work should use the latest V2 packages.

## Differences between this version and the previous SDK

The V2 SDKs bring a few improvements which may break existing code. Reading this section may help port existing Python code that used the lusidtools package.

**secrets.json file replaced by environment variables**

We have improved security by removing the need for a local secrets.json file. Instead, the environment variables need to be set.

See https://support.lusid.com/docs/how-do-i-use-an-api-access-token-with-the-lusid-sdk.

**Signature changes to iam.create_role()**

Is now:
```python 
def create_role(
    access_api_factory: finbourne_access.extensions.SyncApiClientFactory,
    identity_api_factory: finbourne_identity.extensions.SyncApiClientFactory,
    access_role_creation_request: access_models.RoleCreationRequest,
) -> None:
```
Was
```python 
def create_role(
    api_factory: lusid.SyncApiClientFactory,
    access_role_creation_request: access_models.RoleCreationRequest,
):
```
Reason, rework of the ApiClientFactory means the configuration isn't available, thus we now explictity pass in the factory methods for access and identity 

**Lusid.Models, finbourne_access and fibnoure_identity DTOs are now inherited from BaseModel**

Previously all V1 API DTO ( models classes ) were herited from Object, but now V2 API DTOs use pydantic's BaseModel.
1. This allows easier type checking and enumeration using pydantic. This change enforces better type checking at run time. Notable is the type checking on each property such as min max length of a string. Floats and integers can not be string representations in V2, i.e. code can not rely on implicit type conversions 
2. The DTO do not have the properties openapi_types , attribute_map and required_map.  attribute_map has a equivalent __properties. These are not available and are now discovered at runtime. See the  get_attributes_and_types() within finbourne_sdk_utils.cocoon namespace.
3. Each DTO has a Config property, a pydantic configuration technique 
4. Default constructor now expects **kwargs or named parameters. see https://docs.pydantic.dev/1.10/#__tabbed_1_3.


**ApiClient is now async, not synchronous**

Not strictly a finbourne-sdk-utils change as ApiClient is defined in the lusid python SDK. The synchronous class is now call SyncApiClient. The utility functions have been tested synchronously and examples show Synchronous usage. 

**response object from API calls**

SDK APIs now return am ApiResponse object with properties status_code, data and headers, rather than repsonse[0], response[1] etc.

Strictly not a finbourne-sdk-utils tools change, but some utility functions will return the native API response object

**response error handling change**

Serverside errors may now be thrown using ApiException

**Better value checking client side before invoking API**

The client-side code may generate ApiValue exceptions, assignments are now validated to ensure data types are used correctly. Examples include DateTime properties, previously the time part would have defaulted to midnight if only the date was provided, but now client-side checking will ensure it's a correctly formatted ISO 8601 string representation. Likewise, DateTime field will also expect the regional information or Zulu specified.

**Model Objects to_dict() now returns the alias of the object parameters**

V2 of the python SDK uses the BaseModel as the base class for all Model objects used by the APIs. The implementation of the to_dict() now returns the alias version of the property values. The alias is the original name of the parameter as specified in the API swagger definition. Typically, the APIs use camelCase notation, the python SDK will convert these to snake_case. For example, UpsertInstruments response returns 'lusidInstrumentId', but the model object uses lusid_instrument_id, with the alias lusidInstrumentId, i.e.

```python
class Instrument(BaseModel):
   ...
    lusid_instrument_id: constr(strict=True, min_length=1) = Field(..., alias="lusidInstrumentId", description="The unique LUSID Instrument Identifier (LUID) of the instrument.")
    ...
   
```

likewise, when the SDK serialises the Model instances to send to the server, the reverse occurs, i.e. the alias equivalents are used to construct the request body.

As a consequence, the utility functions will also use the alias equivalents, ie.

V2 becomes 
```python
# Transform API response to a dataframe and show internally-generated unique LUID for each mastered instrument
upsert_instruments_response_df = lusid_response_to_data_frame(list(upsert_instruments_response.values.values()))

display(upsert_instruments_response_df[["name", "lusidInstrumentId"]])
```
From previous V1
```python
# Transform API response to a dataframe and show internally-generated unique LUID for each mastered instrument
upsert_instruments_response_df = lusid_response_to_data_frame(list(upsert_instruments_response.values.values()))

display(upsert_instruments_response_df[["name", "lusid_instrument_id"]])
```
