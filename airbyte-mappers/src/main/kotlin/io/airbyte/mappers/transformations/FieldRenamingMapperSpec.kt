package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig

class FieldRenamingMapperSpec : ConfigValidatingSpec<FieldRenamingMapperConfig>() {
  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): FieldRenamingMapperConfig {
    return Jsons.convertValue(configuredMapper, FieldRenamingMapperConfig::class.java)
  }

  override fun jsonSchema(): JsonNode {
    return simpleJsonSchemaGenerator.generateJsonSchema(specType())
  }

  override fun specType(): Class<*> {
    return FieldRenamingMapperConfig::class.java
  }
}
