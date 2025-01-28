/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig

class FieldRenamingMapperSpec : ConfigValidatingSpec<FieldRenamingMapperConfig>() {
  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): FieldRenamingMapperConfig =
    Jsons.convertValue(configuredMapper, FieldRenamingMapperConfig::class.java)

  override fun jsonSchema(): JsonNode = simpleJsonSchemaGenerator.generateJsonSchema(specType())

  override fun specType(): Class<*> = FieldRenamingMapperConfig::class.java
}
