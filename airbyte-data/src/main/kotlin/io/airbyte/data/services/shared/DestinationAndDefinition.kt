/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.DestinationConnection
import io.airbyte.config.StandardDestinationDefinition

/**
 * A pair of a destination connection and its associated definition.
 *
 * @param destination Destination.
 * @param definition Destination definition.
 */
@JvmRecord
data class DestinationAndDefinition(
  @JvmField val destination: DestinationConnection,
  @JvmField val definition: StandardDestinationDefinition,
)
