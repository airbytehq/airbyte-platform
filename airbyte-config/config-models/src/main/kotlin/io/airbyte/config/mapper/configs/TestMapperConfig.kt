/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue
import io.airbyte.config.MapperConfig
import java.util.UUID

const val TEST_MAPPER_NAME = "test-mapper"

data class TestMapperConfig(
  @JsonProperty("name")
  @field:NotNull
  @field:SchemaDescription("The name of the operation.")
  @field:SchemaConstant(TEST_MAPPER_NAME)
  val name: String = TEST_MAPPER_NAME,
  @JsonIgnore
  @field:SchemaDescription("URL for documentation related to this configuration.")
  @field:SchemaFormat("uri")
  val documentationUrl: String? = null,
  val id: UUID? = null,
  @JsonProperty("config")
  @field:NotNull
  val config: TestConfig,
) : MapperConfig {
  override fun name(): String = name

  override fun id(): UUID? = id

  override fun documentationUrl(): String? = documentationUrl

  override fun config(): Any = config
}

data class TestConfig(
  @JsonProperty("field1")
  @field:NotNull
  @field:SchemaTitle("Field One")
  @field:SchemaDescription("Field One")
  val field1: String,
  @JsonProperty("enumField")
  @field:SchemaTitle("Enum Field")
  @field:SchemaDescription("Enum Field")
  @field:SchemaDefault("ONE")
  @field:SchemaExamples("ONE")
  @field:NotNull
  val enumField: TestEnums = TestEnums.ONE,
  @JsonProperty("field2")
  @field:SchemaTitle("Field Two")
  @field:SchemaDescription("Field Two")
  @field:NotNull
  val field2: String,
)

enum class TestEnums(
  private val value: String,
) {
  ONE("ONE"),
  TWO("TWO"),
  ;

  @JsonValue
  override fun toString() = value
}
