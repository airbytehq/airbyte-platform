/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.HashingMapperConfig

class HashingMapperSpec : ConfigValidatingSpec<HashingMapperConfig>() {
  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): HashingMapperConfig =
    Jsons.convertValue(configuredMapper, HashingMapperConfig::class.java)

  override fun jsonSchema(): JsonNode = simpleJsonSchemaGenerator.generateJsonSchema(specType())

  override fun specType(): Class<*> = HashingMapperConfig::class.java
}
