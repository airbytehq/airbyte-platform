/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.MapperOperationName.ENCRYPTION
import io.airbyte.config.MapperOperationName.FIELD_FILTERING
import io.airbyte.config.MapperOperationName.FIELD_RENAMING
import io.airbyte.config.MapperOperationName.HASHING
import io.airbyte.config.MapperOperationName.ROW_FILTERING
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.FieldFilteringMapperConfig
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestMapperConfig
import java.util.UUID

object MapperOperationName {
  const val ENCRYPTION = "encryption"
  const val FIELD_FILTERING = "field-filtering"
  const val FIELD_RENAMING = "field-renaming"
  const val HASHING = "hashing"
  const val ROW_FILTERING = "row-filtering"
}

/**
 * Configured mapper we want to apply.
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "name",
  visible = true,
)
@JsonSubTypes(
  JsonSubTypes.Type(value = TestMapperConfig::class, name = TEST_MAPPER_NAME),
  JsonSubTypes.Type(value = EncryptionMapperConfig::class, name = ENCRYPTION),
  JsonSubTypes.Type(value = HashingMapperConfig::class, name = HASHING),
  JsonSubTypes.Type(value = FieldFilteringMapperConfig::class, name = FIELD_FILTERING),
  JsonSubTypes.Type(value = FieldRenamingMapperConfig::class, name = FIELD_RENAMING),
  JsonSubTypes.Type(value = RowFilteringMapperConfig::class, name = ROW_FILTERING),
)
interface MapperConfig {
  fun name(): String

  fun id(): UUID?

  fun documentationUrl(): String?

  fun config(): Any
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ConfiguredMapper(
  val name: String,
  val config: JsonNode,
  val id: UUID? = null,
)
