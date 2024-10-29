package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig

class RowFilteringMapperSpec : ConfigValidatingSpec<RowFilteringMapperConfig>() {
  private val rowFilterMapperConfigSpecGenerator = RowFilterMapperConfigSpecGenerator()

  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): RowFilteringMapperConfig {
    return Jsons.deserialize(Jsons.serialize(configuredMapper), RowFilteringMapperConfig::class.java)
  }

  override fun jsonSchema(): JsonNode {
    return rowFilterMapperConfigSpecGenerator.generateSchema(specType())
  }

  override fun specType(): Class<*> {
    return RowFilteringMapperConfig::class.java
  }
}
