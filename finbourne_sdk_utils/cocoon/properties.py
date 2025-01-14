from typing import List

from finbourne_sdk_utils.cocoon.utilities import checkargs
from finbourne_sdk_utils import cocoon
import lusid
import pandas as pd
import logging
from http import HTTPStatus
import numpy as np

# Map Numpy data types to LUSID data types
global_constants = {
    "data_type_mapping": {
        "object": "string",
        "float64": "number",
        "int64": "number",
        "bool": "string",
        "datetime64[ns, UTC]": "string",
    }
}


@checkargs
def check_property_definitions_exist_in_scope_single(
    api_factory: lusid.SyncApiClientFactory, property_key: str
) -> tuple[bool, str]:
    """
    This function takes a list of property keys and looks to see which property definitions already exist inside LUSID

    Parameters
    ----------
    api_factory : lusid.SyncApiClientFactory
        The ApiFactory to use
    property_key: str
        The property key to get from LUSID

    Returns
    -------
    exists : bool
        Whether or not the property definition exists
    data_type : str
        property definition's data type
    """

    data_type = None

    try:
        response = api_factory.build(
            lusid.PropertyDefinitionsApi
        ).get_property_definition(
            domain=property_key.split("/")[0],
            scope=property_key.split("/")[1],
            code=property_key.split("/")[2],
        )

        exists = True
        data_type = response.data_type_id.code

    except lusid.exceptions.ApiException as ex:
        if ex.status == HTTPStatus.NOT_FOUND:
            exists = False
        else:
            raise

    return exists, data_type


@checkargs
def check_property_definitions_exist_in_scope(
    api_factory: lusid.SyncApiClientFactory,
    domain: str,
    data_frame: pd.DataFrame,
    target_columns: list,
    column_to_scope: dict,
):
    """
    This function identifiers which property definitions are missing from LUSID

    Parameters
    ----------
    api_factory :   lusid.SyncApiClientFactory
        The Api Factory to use
    domain : str
        The domain to check for property definitions in
    data_frame : pd.DataFrame
        The dataframe to check properties for
    target_columns : list[str]
        The columns to add properties for
    column_to_scope : dict[str:str]
        Column name to scope
    Returns
    -------
    missing_property_columns :  list[str]
        The columns missing properties in LUSID
    data_frame : pd.DataFrame
        The input DataFrame with types updated
    """

    data_type_update_map = {"number": "float64", "string": "object"}

    # Initialise a set to hold the missing properties
    missing_keys = set([])

    # Iterate over the column names
    column_property_mapping = {}

    for column_name, data_type in data_frame.loc[:, target_columns].dtypes.items():

        # Create the property key
        property_key = f"{domain}/{column_to_scope[column_name]}/{cocoon.utilities.make_code_lusid_friendly(column_name)}"

        column_property_mapping[property_key] = column_name

        # Get a tuple with the first value being True/False key is missing, second is data type of the key
        exists, data_type_lusid = check_property_definitions_exist_in_scope_single(
            api_factory=api_factory, property_key=property_key
        )

        # If the key is missing add it to the set
        if not exists:
            missing_keys.add(property_key)

        # If it is not missing check that the data type of the property matches the dataframe
        else:
            # If the data type does not match
            if data_type_lusid != global_constants["data_type_mapping"][str(data_type)]:
                logging.warning(
                    f"Data types don't match for column {column_name} it is {data_type_lusid} in LUSID and {data_type} in file"
                )
                try:
                    updated_data_type = data_type_update_map[data_type_lusid]
                except KeyError:
                    updated_data_type = "object"

                # Update the data type in the dataframe if possible
                data_frame[column_name] = data_frame[column_name].astype(
                    updated_data_type, copy=False
                )

                logging.info(f"Updated {column_name} to {updated_data_type}")

    missing_property_columns = [
        column_property_mapping[property_key] for property_key in missing_keys
    ]

    return missing_property_columns, data_frame


@checkargs
def create_property_definitions_from_file(
    api_factory: lusid.SyncApiClientFactory,
    domain: str,
    data_frame: pd.DataFrame,
    missing_property_columns: list,
    column_to_scope: dict,
):
    """
    Creates the property definitions for all the columns in a file

    Parameters
    ----------
    api_factory : lusid.SyncApiClientFactory
        The ApiFactory to use
    domain : domain
        The domain to create the property definitions in
    data_frame : pd.Series
        The dataframe dtypes to add definitions for
    missing_property_columns : list[str]
        The columns that property defintions are missing for
    column_to_scope : dict[str:str]
        Column name to scope

    Returns
    -------
    property_key_mapping : dict
        A mapping of data_frame columns to property keys
    """

    missing_property_data_frame = data_frame.loc[:, missing_property_columns]

    # Ensure that all data types in the file have been mapped
    actual_data_types = set(
        [str(data_type) for data_type in missing_property_data_frame.dtypes]
    )
    allowed_data_types = set(global_constants["data_type_mapping"])
    if not (actual_data_types <= allowed_data_types):
        unmapped_data_types = [
            np.dtype(value) for value in actual_data_types - allowed_data_types
        ]
        unmapped_columns = missing_property_data_frame.dtypes[
            missing_property_data_frame.dtypes.isin(unmapped_data_types)
        ]
        raise TypeError(
            invalid_columns_error_message(unmapped_columns, allowed_data_types)
        )

    # Initialise a dictionary to hold the keys
    property_key_mapping = {}

    # Iterate over the each column and its data type
    for column_name, data_type in missing_property_data_frame.dtypes.items():

        # Make the column name LUSID friendly
        lusid_friendly_code = cocoon.utilities.make_code_lusid_friendly(column_name)

        # If there is no data Pandas infers a type of float, would prefer to infer object
        if missing_property_data_frame[column_name].isnull().all():
            logging.warning(
                f"{column_name} is null, no type can be inferred it will be treated as a string"
            )
            data_type = "object"
            data_frame[column_name] = data_frame[column_name].astype(
                "object", copy=False
            )

        # Create a request to define the property, assumes value_required is false for all
        property_request = lusid.models.CreatePropertyDefinitionRequest(
            domain=domain,
            scope=column_to_scope[column_name],
            code=lusid_friendly_code,
            value_required=False,
            display_name=column_name,
            data_type_id=lusid.models.ResourceId(
                scope="system",
                code=global_constants["data_type_mapping"][str(data_type)],
            ),
        )

        # Call LUSID to create the new property
        property_response = api_factory.build(
            lusid.PropertyDefinitionsApi
        ).create_property_definition(
            create_property_definition_request=property_request
        )

        logging.info(
            f"Created - {property_response.key} - with datatype {property_response.data_type_id.code}"
        )

        # Grab the key off the response to use when referencing this property in other LUSID calls
        property_key_mapping[column_name] = property_response.key

    return property_key_mapping, data_frame


@checkargs
def create_missing_property_definitions_from_file(
    api_factory: lusid.SyncApiClientFactory,
    properties_scope: str,
    data_frame: pd.DataFrame,
    property_columns: list,
    domain: str,
):
    # If there are property columns
    if len(property_columns) > 0 and domain is not None:

        source_columns = [column["source"] for column in property_columns]
        source_to_target = {
            column1["source"]: column1.get("target", column1.get("source"))
            for column1 in property_columns
        }

        for column in source_columns:
            data_frame.loc[:, source_to_target[column]] = data_frame[column]

        target_columns = [
            column.get("target", column.get("source")) for column in property_columns
        ]
        column_to_scope = {
            column.get("target", column.get("source")): column.get(
                "scope", properties_scope
            )
            for column in property_columns
        }

        # Identify which property definitions are missing
        (
            missing_property_columns,
            data_frame,
        ) = cocoon.properties.check_property_definitions_exist_in_scope(
            api_factory=api_factory,
            domain=domain,
            data_frame=data_frame,
            target_columns=target_columns,
            column_to_scope=column_to_scope,
        )

        logging.info(
            f"Check for missing {domain} properties complete. {len(missing_property_columns)} missing properties found"
        )

        # If there are missing property definitions
        if len(missing_property_columns) > 0:
            logging.info(
                f"The {domain} properties {str(missing_property_columns)} will be added in the scope {properties_scope}"
            )

            # Create property definitions for all of the columns in the file that have missing definitions
            (
                property_key_mapping,
                data_frame,
            ) = cocoon.properties.create_property_definitions_from_file(
                api_factory=api_factory,
                domain=domain,
                data_frame=data_frame,
                missing_property_columns=missing_property_columns,
                column_to_scope=column_to_scope,
            )

    return data_frame


def invalid_columns_error_message(unmapped_columns, allowed_data_types):
    formatted_unmapped_columns = {
        k: str(v) for k, v in unmapped_columns.to_dict().items()
    }
    return f"""The following columns in the data_frame have not been mapped to LUSID data types: {formatted_unmapped_columns}.
                           LUSID supports the following data types: {allowed_data_types}. 
                           Please ensure that all data types have been mapped before retrying."""


@checkargs
def create_property_values(
    row: pd.Series, column_to_scope: dict, scope: str, domain: str, dtypes: pd.Series
) -> dict:
    """
    This function generates the property values for a row in a file

    Parameters
    ----------
    row : pd.Series
        The current row of the data frame to create property values for
    column_to_scope : dict {str, str}
        The scope for a column name
    scope : str
        The domain to create the property values in
    domain : str
        The domain to create the property values in
    dtypes : pd.Series
        The data types of each column to create property values for

    Returns
    -------
    properties : dict {str, models.PerpetualProperty}
    """

    actual_data_types = set([str(data_type) for data_type in dtypes])
    allowed_data_types = set(global_constants["data_type_mapping"])

    # Ensure that all data types in the file have been mapped
    if not (actual_data_types <= allowed_data_types):
        unmapped_data_types = actual_data_types - allowed_data_types
        unmapped_columns = dtypes[dtypes.isin(unmapped_data_types)]
        raise TypeError(
            invalid_columns_error_message(unmapped_columns, allowed_data_types)
        )

    # Initialise the empty properties dictionary
    properties = {}

    # Iterate over each column name and data type
    for column_name, data_type in dtypes.items():

        # Set the data type to be a string so that it is easier to work with
        string_data_type = str(data_type)
        # Convert the numpy data type to a LUSID data type using the global mapping
        lusid_data_type = global_constants["data_type_mapping"][string_data_type]
        # Get the value of the column from the row
        row_value = row[column_name]

        # Use the correct LUSID property value based on the data type
        if lusid_data_type == "string":
            if pd.isna(row_value):
                continue
            property_value = lusid.models.PropertyValue(label_value=str(row_value))  

        if lusid_data_type == "number":
            # Handle null values given the input null value override
            if pd.isnull(row_value):
                continue
            property_value = lusid.models.PropertyValue(
                metric_value=lusid.models.MetricValue(value=row_value)
            )

        # Set the property
        property_key = f"{domain}/{column_to_scope.get(column_name, scope)}/{cocoon.utilities.make_code_lusid_friendly(column_name)}"
        properties[property_key] = lusid.models.PerpetualProperty(
            key=property_key, value=property_value
        )

    if domain.lower() == "instrument":
        properties = list(properties.values())

    return properties


def _infer_full_property_keys(
    partial_keys: list, properties_scope: str, domain: str
) -> list:
    """
    Infers from a list of partially completed property keys the entire property key

    Parameters
    ----------
    partial_keys : list[str]
        The partial keys
    properties_scope : str
        The scope of the properties
    domain : str
        The domain of the properties
    Returns
    -------
    list[str]
        A list of full property keys
    """

    full_keys = []

    for key in partial_keys:

        split_key = key.split("/")
        number_components = len(split_key)
        # The entire key is already specified
        if number_components == 3:
            full_keys.append(key)
        # The domain is missing with the scope and code specified
        elif number_components == 2:
            full_keys.append(f"{domain}/{split_key[0]}/{split_key[1]}")
        # The domain and scope are missing with only the code specified
        elif number_components == 1:
            full_keys.append(f"{domain}/{properties_scope}/{key}")

    # Ensure that the returned keys are LUSID friendly
    return [
        "/".join(
            [
                cocoon.utilities.make_code_lusid_friendly(partial)
                for partial in key.split("/")
            ]
        )
        for key in full_keys
    ]
