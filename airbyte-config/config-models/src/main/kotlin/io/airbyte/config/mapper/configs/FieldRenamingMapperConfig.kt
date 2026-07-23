/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import java.util.UUID

data class FieldRenamingMapperConfig(
  @JsonProperty("name")
  @field:NotNull
  @field:SchemaDescription("The name of the operation.")
  @field:SchemaConstant(MapperOperationName.FIELD_RENAMING)
  val name: String = MapperOperationName.FIELD_RENAMING,
  @JsonIgnore
  @field:SchemaDescription("URL for documentation related to this configuration.")
  @field:SchemaFormat("uri")
  val documentationUrl: String? = null,
  @JsonProperty("config")
  @field:NotNull
  val config: FieldRenamingConfig,
  val id: UUID? = null,
) : MapperConfig {
  override fun name(): String = name

  override fun documentationUrl(): String? = documentationUrl

  override fun id(): UUID? = id

  override fun config(): Any = config
}

data class FieldRenamingConfig(
  @JsonProperty("originalFieldName")
  @field:NotNull
  @field:SchemaDescription("The current name of the field to rename.")
  @field:SchemaTitle("Original Field Name")
  val originalFieldName: String,
  @JsonProperty("newFieldName")
  @field:NotNull
  @field:SchemaDescription("The new name for the field after renaming.")
  @field:SchemaTitle("New Field Name")
  val newFieldName: String,
)
