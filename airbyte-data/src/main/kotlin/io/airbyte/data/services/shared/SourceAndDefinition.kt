/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition

/**
 * Pair of source and its associated definition.
 *
 *
 * Data-carrier records to hold combined result of query for a Source or Destination and its
 * corresponding Definition. This enables the API layer to process combined information about a
 * Source/Destination/Definition pair without requiring two separate queries and in-memory join
 * operation, because the config models are grouped immediately in the repository layer.
 *
 * @param source source
 * @param definition its corresponding definition
 */
@JvmRecord
data class SourceAndDefinition(
  @JvmField val source: SourceConnection,
  @JvmField val definition: StandardSourceDefinition,
)
