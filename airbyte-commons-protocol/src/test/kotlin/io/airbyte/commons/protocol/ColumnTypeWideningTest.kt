/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.ColumnTypeWidening.WidenedColumn
import io.airbyte.commons.protocol.ColumnTypeWidening.mergeStreamSchema
import io.airbyte.commons.protocol.ColumnTypeWidening.widen
import io.airbyte.config.FieldType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource

internal class ColumnTypeWideningTest {
  @ParameterizedTest
  @EnumSource(FieldType::class)
  fun `every type reaches UNKNOWN`(type: FieldType) {
    assertEquals(FieldType.UNKNOWN, widen(type, FieldType.UNKNOWN))
  }

  @ParameterizedTest
  @MethodSource("supertypes")
  fun `widen returns the closest shared parent in both directions`(
    a: FieldType,
    b: FieldType,
    expected: FieldType,
  ) {
    assertEquals(expected, widen(a, b))
    assertEquals(expected, widen(b, a))
  }

  @Test
  fun `narrowing keeps the saved type and records the source type`() {
    val result = merge(saved = column("string"), discovered = column("number", airbyteType = "integer"))

    assertEquals(Jsons.jsonNode(mapOf("type" to "string", "airbyte_source_type" to "integer")), result.order())
    assertEquals(listOf(WidenedColumn(ORDER, FieldType.STRING, FieldType.INTEGER)), result.widenedColumns)
  }

  @Test
  fun `widening to the new type takes the discovered column without the key`() {
    val result = merge(saved = column("number", airbyteType = "integer"), discovered = column("string"))

    assertEquals(column("string"), result.order())
    assertEquals(emptyList<WidenedColumn>(), result.widenedColumns)
  }

  @Test
  fun `an unchanged database type keeps the widened column`() {
    val widened = column("string", sourceType = "integer")
    val result = merge(saved = widened, discovered = column("number", airbyteType = "integer"))

    assertEquals(widened, result.order())
    assertEquals(emptyList<WidenedColumn>(), result.widenedColumns)
  }

  @Test
  fun `a database type matching the saved type removes the key`() {
    val result = merge(saved = column("string", sourceType = "integer"), discovered = column("string"))

    assertEquals(column("string"), result.order())
  }

  @Test
  fun `a widened column whose database type changes again updates the key`() {
    val result = merge(saved = column("string", sourceType = "integer"), discovered = column("number"))

    assertEquals(column("string", sourceType = "number"), result.order())
    assertEquals(listOf(WidenedColumn(ORDER, FieldType.STRING, FieldType.NUMBER)), result.widenedColumns)
  }

  @Test
  fun `a supertype that is neither type is written from the field type`() {
    val result = merge(saved = column("boolean"), discovered = column("number", airbyteType = "integer"))

    assertEquals(column("string", sourceType = "integer"), result.order())
  }

  @Test
  fun `unknown is a real supertype`() {
    val result = merge(saved = column("string"), discovered = column("object"))

    assertEquals(column("unknown", sourceType = "object"), result.order())
  }

  @Test
  fun `a discovered type that can't be classified is taken as is`() {
    val discovered = Jsons.jsonNode(mapOf("type" to "string", "format" to "uuid"))
    val result = merge(saved = column("string"), discovered = discovered)

    assertEquals(discovered, result.order())
    assertEquals(emptyList<WidenedColumn>(), result.widenedColumns)
  }

  @Test
  fun `primary key and cursor columns are never widened`() {
    val result =
      mergeStreamSchema(
        savedSchema = schema(ORDER to column("string")),
        discoveredSchema = schema(ORDER to column("number", airbyteType = "integer")),
        keyColumns = setOf(ORDER),
      )

    assertEquals(column("number", airbyteType = "integer"), result.order())
    assertEquals(emptyList<WidenedColumn>(), result.widenedColumns)
  }

  @Test
  fun `added and removed columns follow the discovered schema`() {
    val result =
      mergeStreamSchema(
        savedSchema = schema(ORDER to column("string"), "old" to column("string")),
        discoveredSchema = schema(ORDER to column("string"), "notes" to column("string")),
        keyColumns = emptySet(),
      )

    assertEquals(schema(ORDER to column("string"), "notes" to column("string")), result.jsonSchema)
  }

  @Test
  fun `the saved schema is not modified`() {
    val savedSchema = schema(ORDER to column("string", sourceType = "integer"))
    val savedCopy = savedSchema.deepCopy<JsonNode>()

    mergeStreamSchema(savedSchema, schema(ORDER to column("number")), emptySet())

    assertEquals(savedCopy, savedSchema)
  }

  @Test
  fun `a discovered schema without properties is returned as is`() {
    val discovered = Jsons.jsonNode(mapOf("type" to "object"))

    val result = mergeStreamSchema(schema(ORDER to column("string")), discovered, emptySet())

    assertEquals(discovered, result.jsonSchema)
    assertEquals(emptyList<WidenedColumn>(), result.widenedColumns)
  }

  private fun merge(
    saved: JsonNode,
    discovered: JsonNode,
  ): ColumnTypeWidening.MergeResult = mergeStreamSchema(schema(ORDER to saved), schema(ORDER to discovered), emptySet())

  private fun ColumnTypeWidening.MergeResult.order(): JsonNode = jsonSchema.get("properties").get(ORDER)

  private fun column(
    type: String,
    airbyteType: String = "",
    sourceType: String = "",
  ): JsonNode =
    Jsons.jsonNode(
      buildMap {
        put("type", type)
        if (airbyteType.isNotEmpty()) put("airbyte_type", airbyteType)
        if (sourceType.isNotEmpty()) put("airbyte_source_type", sourceType)
      },
    )

  private fun schema(vararg columns: Pair<String, JsonNode>): JsonNode = Jsons.jsonNode(mapOf("type" to "object", "properties" to columns.toMap()))

  companion object {
    private const val ORDER = "order_id"

    @JvmStatic
    fun supertypes(): List<Arguments> =
      listOf(
        Arguments.of(FieldType.STRING, FieldType.STRING, FieldType.STRING),
        Arguments.of(FieldType.STRING, FieldType.INTEGER, FieldType.STRING),
        Arguments.of(FieldType.INTEGER, FieldType.NUMBER, FieldType.NUMBER),
        Arguments.of(FieldType.BOOLEAN, FieldType.INTEGER, FieldType.STRING),
        Arguments.of(FieldType.DATE, FieldType.TIMESTAMP_WITH_TIMEZONE, FieldType.TIMESTAMP_WITH_TIMEZONE),
        Arguments.of(FieldType.DATE, FieldType.TIMESTAMP_WITHOUT_TIMEZONE, FieldType.TIMESTAMP_WITHOUT_TIMEZONE),
        Arguments.of(FieldType.TIME_WITHOUT_TIMEZONE, FieldType.TIME_WITH_TIMEZONE, FieldType.TIME_WITH_TIMEZONE),
        Arguments.of(FieldType.DATE, FieldType.TIME_WITH_TIMEZONE, FieldType.STRING),
        Arguments.of(FieldType.STRING, FieldType.OBJECT, FieldType.UNKNOWN),
        Arguments.of(FieldType.ARRAY, FieldType.OBJECT, FieldType.UNKNOWN),
        Arguments.of(FieldType.MULTI, FieldType.INTEGER, FieldType.UNKNOWN),
      )
  }
}
