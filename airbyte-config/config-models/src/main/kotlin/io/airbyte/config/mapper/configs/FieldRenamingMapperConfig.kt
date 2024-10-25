package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName

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
) : MapperConfig {
  override fun name(): String {
    return name
  }

  override fun documentationUrl(): String? {
    return documentationUrl
  }

  override fun config(): Any {
    return config
  }
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
