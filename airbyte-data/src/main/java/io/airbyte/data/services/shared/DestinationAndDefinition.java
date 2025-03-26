/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import io.airbyte.config.DestinationConnection;
import io.airbyte.config.StandardDestinationDefinition;

/**
 * A pair of a destination connection and its associated definition.
 *
 * @param destination Destination.
 * @param definition Destination definition.
 */
public record DestinationAndDefinition(DestinationConnection destination, StandardDestinationDefinition definition) {

}
