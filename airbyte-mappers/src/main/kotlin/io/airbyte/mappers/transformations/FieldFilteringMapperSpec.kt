/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.FieldFilteringMapperConfig

class FieldFilteringMapperSpec(
  objectMapper: ObjectMapper,
) : ConfigValidatingSpec<FieldFilteringMapperConfig>(objectMapper) {
  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): FieldFilteringMapperConfig =
    objectMapper().convertValue(configuredMapper, FieldFilteringMapperConfig::class.java)

  override fun jsonSchema(): JsonNode = simpleJsonSchemaGenerator.generateJsonSchema(specType())

  override fun specType(): Class<*> = FieldFilteringMapperConfig::class.java
}
