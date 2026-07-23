/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import java.util.UUID

data class HashingMapperConfig(
  @JsonProperty("name")
  @field:NotNull
  @field:SchemaDescription("The name of the operation.")
  @field:SchemaConstant(MapperOperationName.HASHING)
  val name: String = MapperOperationName.HASHING,
  @JsonIgnore
  @field:SchemaDescription("URL for documentation related to this configuration.")
  @field:SchemaFormat("uri")
  val documentationUrl: String? = null,
  @JsonProperty("config")
  @field:NotNull
  val config: HashingConfig,
  val id: UUID? = null,
) : MapperConfig {
  override fun name(): String = name

  override fun documentationUrl(): String? = documentationUrl

  override fun id(): UUID? = id

  override fun config(): HashingConfig = config
}

enum class HashingMethods(
  val value: String,
) {
  MD2("MD2"),
  MD5("MD5"),
  SHA1("SHA-1"),
  SHA224("SHA-224"),
  SHA256("SHA-256"),
  SHA384("SHA-384"),
  SHA512("SHA-512"),
  ;

  @JsonValue
  override fun toString() = value

  companion object {
    fun fromValue(value: String): HashingMethods? = entries.find { it.value == value }
  }
}

data class HashingConfig(
  @JsonProperty("targetField")
  @field:NotNull
  @field:SchemaTitle("Original Field Name")
  @field:SchemaDescription("The name of the field to be hashed.")
  val targetField: String,
  @JsonProperty("method")
  @field:SchemaTitle("Hashing method")
  @field:SchemaDescription("The hashing algorithm to use.")
  @field:SchemaDefault("SHA-256")
  @field:SchemaExamples("SHA-256")
  @field:NotNull
  val method: HashingMethods,
  @JsonProperty("fieldNameSuffix")
  @field:SchemaTitle("Field name suffix")
  @field:SchemaDescription("The suffix to append to the field name after hashing.")
  @field:SchemaDefault("_hashed")
  @field:NotNull
  val fieldNameSuffix: String,
)
