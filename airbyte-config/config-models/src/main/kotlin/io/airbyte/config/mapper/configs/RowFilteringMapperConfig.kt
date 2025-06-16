/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import java.util.UUID

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type",
)
@JsonSubTypes(
  JsonSubTypes.Type(value = AndOperation::class, names = ["AND", "and"]),
  JsonSubTypes.Type(value = OrOperation::class, names = ["OR", "or"]),
  JsonSubTypes.Type(value = EqualOperation::class, names = ["EQUAL", "equal"]),
  JsonSubTypes.Type(value = NotOperation::class, names = ["NOT", "not"]),
)
sealed class Operation {
  abstract val type: String
}

data class NotOperation(
  @JsonProperty("type")
  @field:NotNull
  @field:SchemaConstant("NOT")
  override val type: String = "NOT",
  @JsonProperty("conditions")
  @field:NotNull
  @field:SchemaTitle("Sub-Conditions (NOT)")
  @field:SchemaDescription("Conditions to evaluate with the NOT operator.")
  val conditions: List<Operation>,
) : Operation()

data class EqualOperation(
  @JsonProperty("type")
  @field:NotNull
  @field:SchemaConstant("EQUAL")
  override val type: String = "EQUAL",
  @JsonProperty("fieldName")
  @field:NotNull
  @field:SchemaTitle("Field Name")
  @field:SchemaDescription("The name of the field to apply the operation on.")
  val fieldName: String,
  @JsonProperty("comparisonValue")
  @field:NotNull
  @field:SchemaTitle("Comparison Value")
  @field:SchemaDescription("The value to compare the field against.")
  val comparisonValue: String,
) : Operation()

data class OrOperation(
  @JsonProperty("type")
  @field:NotNull
  @field:SchemaConstant("OR")
  override val type: String = "OR",
  @JsonProperty("conditions")
  @field:NotNull
  @field:SchemaTitle("Sub-Conditions (OR)")
  @field:SchemaDescription("Conditions to evaluate with the OR operator.")
  val conditions: List<Operation>,
) : Operation()

data class AndOperation(
  @JsonProperty("type")
  @field:NotNull
  @field:SchemaConstant("AND")
  override val type: String = "AND",
  @JsonProperty("conditions")
  @field:NotNull
  @field:SchemaTitle("Sub-Conditions (AND)")
  @field:SchemaDescription("Conditions to evaluate with the AND operator.")
  val conditions: List<Operation>,
) : Operation()

data class RowFilteringConfig(
  @JsonProperty("conditions")
  @field:SchemaTitle("Conditions")
  @field:SchemaDescription("Defines conditions for including records with logical and nested operations.")
  @field:NotNull
  val conditions: Operation,
)

data class RowFilteringMapperConfig(
  @JsonProperty("name")
  @field:NotNull
  @field:SchemaDescription("The name of the operation.")
  @field:SchemaConstant(MapperOperationName.ROW_FILTERING)
  val name: String = MapperOperationName.ROW_FILTERING,
  @JsonIgnore
  @field:SchemaDescription("URL for documentation related to this configuration.")
  @field:SchemaFormat("uri")
  val documentationUrl: String? = null,
  @JsonProperty("config")
  @field:NotNull
  val config: RowFilteringConfig,
  val id: UUID? = null,
) : MapperConfig {
  override fun name(): String = name

  override fun documentationUrl(): String? = documentationUrl

  override fun id(): UUID? = id

  override fun config(): Any = config
}
