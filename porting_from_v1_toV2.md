![LUSID_by_Finbourne](./resources/Finbourne_Logo_Teal.svg)

# Python tools for LUSID

This package contains a set of utility functions and a Command Line Interface (CLI) for interacting with [LUSID by FINBOURNE](https://www.finbourne.com/lusid-technology). To use it you'll need a LUSID account. [Sign up for free at lusid.com](https://www.lusid.com/app/signup)


## Tips for using the V2 of the Lusid Python Tools
Existing code which used the original LusidPythonTool ( known as V1 or preview) library may no longer work
The following information can be used to help port notebooks or your own code to use this version of the Lusid Python Tools  

## Why upgrade
This package is now based upon the V2 of the Lusid Python SDK. The latest V2 brings many enhancements and language improvements.  V1 of the SDKs are now depreciated and hence any new work should use the latest V2 packages.

## Differences between this version and the previous SDK
The V2 SDKs bring a few improvements which may break existing code. Reading this section may help port existing python code which used the V1 or preview version of the package.


## secrets.json file replaced by environment variables
We have improved the security by removing the need for a local secrets.json file. Instead the environment variables need to be set

See https://support.lusid.com/docs/how-do-i-use-an-api-access-token-with-the-lusid-sdk

**Signature changes to iam.create_role**
Is now 
def create_role(
    access_api_factory: finbourne_access.extensions.SyncApiClientFactory,
    identity_api_factory: finbourne_identity.extensions.SyncApiClientFactory,
    access_role_creation_request: access_models.RoleCreationRequest,
) -> None:

Was

def create_role(
    api_factory: lusid.SyncApiClientFactory,
    access_role_creation_request: access_models.RoleCreationRequest,
):
Reason, rework of the ApiClientFactory means the configuration isn't available, thus we now explictity pass in the factory methods for access and identity 

**Lusid.Models, finbourne_access and fibnoure_identity DTOs are now inherited from BaseModel**

Previously all API DTO ( models classes ) were herited from Object, but now we use pydantic's BaseModel.
1. This allows easier type checking and enumeration using pydantic. This change enforces better type checking at run time. Notable is the type checking on each property such as min max length of a string. Floats and integers can not be string representations in V2, i.e. code can not rely on implicit type conversions 
2. The DTO do not have the properties openapi_types , attribute_map and required_map.  attribute_map has a equivalent __properties. These are not available and are now discovered at runtime. See the  get_attributes_and_types() within lusidtools.cocoon namespace.
3. Each DTO has a Config property, a pydantic configuration technique ( todo concorse ?)
4. Default constructor now expects **kwargs or named parameters. see https://docs.pydantic.dev/1.10/#__tabbed_1_3.



**ApiClient is now async, not synchronous**

Not strictly a lusid python tools change as ApiClient is defined in the lusid python SDK. The synchronous class is now SyncApiClient. Lusid Python Tools has been tested synchronously and examples show Synchronous usage. 

**response object from API calls is now returns am ApiResponse object with properties status_code, data and headers, rather than repsonse[0], response[1] etc.**
Again strictly not a lusid python tools change, but we may expose this return type

**Better value checking client side before invoking API**
The client side code may check more ApiValue exceptions rather than server side data validation errors. Examples include DateTime properties, previously the time part would have defaulted is only the date was provide, but now client side checking will check it's a correctly ISO 8601 formatted string respresentation. Likewise DateTime field will also expect the regional information or Zulu specified.

**response error handling change**
Serverside errors may now be thrown using ApiException (todo qualify)