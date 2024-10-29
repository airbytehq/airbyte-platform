package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.MapperOperationName.FIELD_RENAMING
import io.airbyte.config.MapperOperationName.HASHING
import io.airbyte.config.MapperOperationName.ROW_FILTERING
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestMapperConfig

object MapperOperationName {
  const val HASHING = "hashing"
  const val FIELD_RENAMING = "field-renaming"
  const val ROW_FILTERING = "row-filtering"
}

/**
 * Configured mapper we want to apply.
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "name",
)
@JsonSubTypes(
  JsonSubTypes.Type(value = TestMapperConfig::class, name = TEST_MAPPER_NAME),
  JsonSubTypes.Type(value = HashingMapperConfig::class, name = HASHING),
  JsonSubTypes.Type(value = FieldRenamingMapperConfig::class, name = FIELD_RENAMING),
  JsonSubTypes.Type(value = RowFilteringMapperConfig::class, name = ROW_FILTERING),
)
interface MapperConfig {
  fun name(): String

  fun documentationUrl(): String?

  fun config(): Any
}

data class ConfiguredMapper(
  val name: String,
  val config: JsonNode,
)
