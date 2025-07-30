/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import java.util.UUID

data class FieldFilteringMapperConfig(
  @JsonProperty("name")
  @field:NotNull
  @field:SchemaDescription("The name of the operation.")
  @field:SchemaConstant(MapperOperationName.FIELD_FILTERING)
  val name: String = MapperOperationName.FIELD_FILTERING,
  @JsonIgnore
  @field:SchemaDescription("URL for documentation related to this configuration.")
  @field:SchemaFormat("uri")
  val documentationUrl: String? = null,
  @JsonProperty("config")
  @field:NotNull
  val config: FieldFilteringConfig,
  val id: UUID? = null,
) : MapperConfig {
  override fun name(): String = name

  override fun id(): UUID? = id

  override fun documentationUrl(): String? = documentationUrl

  override fun config(): FieldFilteringConfig = config
}

data class FieldFilteringConfig(
  @JsonProperty("targetField")
  @field:NotNull
  @field:SchemaTitle("The Field To Remove")
  @field:SchemaDescription("The name of the field to be removed.")
  val targetField: String,
)
