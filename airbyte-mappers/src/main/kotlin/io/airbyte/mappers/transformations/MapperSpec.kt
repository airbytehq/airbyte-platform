/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig

interface MapperSpec<T : MapperConfig> {
  fun deserialize(configuredMapper: ConfiguredMapper): T

  fun jsonSchema(): JsonNode

  fun specType(): Class<*>

  fun objectMapper(): ObjectMapper
}
