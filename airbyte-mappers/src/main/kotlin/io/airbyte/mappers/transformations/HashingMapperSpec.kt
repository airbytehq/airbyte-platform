package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.HashingMapperConfig

class HashingMapperSpec : ConfigValidatingSpec<HashingMapperConfig>() {
  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): HashingMapperConfig {
    return Jsons.convertValue(configuredMapper, HashingMapperConfig::class.java)
  }

  override fun jsonSchema(): JsonNode {
    return simpleJsonSchemaGenerator.generateJsonSchema(specType())
  }

  override fun specType(): Class<*> {
    return HashingMapperConfig::class.java
  }
}
