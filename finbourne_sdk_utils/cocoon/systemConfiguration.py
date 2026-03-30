import finbourne.sdk.services.lusid as lusid


def set_transaction_mapping(client, transaction_mapping):
    """
    Sets the transaction mapping in LUSID so that the system can resolve the transactions into movements

    Parameters
    ----------
    client : ApiFactory
        The LusidApi client to use
    transaction_mapping : dict
        The transaction mapping configuration

    Returns
    -------
    response : lusid.ResourceListOfTransactionConfigurationData
        The response from LUSID
    """

    # Initialise your list of configuration requests, one for each transaction type
    configuration_requests = []

    # Iterate over your configurations in the default mapping
    for configuration in transaction_mapping["values"]:

        # Initialise your list of aliases for this configuration
        aliases = []

        # Iterate over the aliases in the imported config
        for alias in configuration["aliases"]:
            # Append the alias to your list
            aliases.append(
                lusid.TransactionConfigurationTypeAlias(
                    type=alias["type"],
                    description=alias["description"],
                    transaction_class=alias["transactionClass"],
                    transaction_group=alias["transactionGroup"],
                    transaction_roles=alias["transactionRoles"],
                )
            )

        # Initialise your list of movements for this configuration
        movements = []

        # Iterate over the movements in the impoted config
        for movement in configuration["movements"]:

            # Add properties if they exist in the config
            if len(movement["properties"]) > 0:
                key = movement["properties"][0]["key"]
                value = lusid.PropertyValue(
                    label_value=movement["properties"][0]["value"]
                )
                properties = {key: lusid.PerpetualProperty(key=key, value=value)}
            else:
                properties = {}

            if len(movement["mappings"]) > 0:
                mappings = [
                    lusid.TransactionPropertyMappingRequest(
                        property_key=movement["mappings"][0]["propertyKey"],
                        set_to=movement["mappings"][0]["setTo"],
                    )
                ]
            else:
                mappings = []

            # Append the movement to your list
            movements.append(
                lusid.TransactionConfigurationMovementDataRequest(
                    movement_types=movement["movementTypes"],
                    side=movement["side"],
                    direction=movement["direction"],
                    properties=properties,
                    mappings=mappings,
                )
            )

        # Build your configuration for this transaction type
        configuration_requests.append(
            lusid.TransactionConfigurationDataRequest(
                aliases=aliases, movements=movements, properties=None
            )
        )

    # Call LUSID to set your configuration for our transaction types
    response = client.system_configuration.set_configuration_transaction_types(
        types=configuration_requests
    )

    return response
