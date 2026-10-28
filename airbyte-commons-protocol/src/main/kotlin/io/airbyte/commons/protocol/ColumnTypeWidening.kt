/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FieldType
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_SOURCE_TYPE
import io.airbyte.config.JsonsSchemaConstants.PROPERTIES
import io.airbyte.config.helpers.FieldGenerator

/**
 * When the source changes a column's type, keeps the column at a supertype that fits both the old and the new values,
 * and records the source's type in `airbyte_source_type`. The source reads with `airbyte_source_type` and converts
 * each value to `type`; the destination only sees `type`.
 */
object ColumnTypeWidening {
  // The supertype chart from the widening spec, as child -> parent. The parent is the next wider type that can hold
  // every value of the child: an integer is a valid number, and any number can be written as a string. Following
  // parents always ends at UNKNOWN, the one type that accepts any value. STRING, OBJECT, ARRAY and MULTI point straight
  // to it because nothing narrower can hold, say, both a string and an object.
  //
  // UNKNOWN
  // ├── STRING
  // │   ├── BOOLEAN
  // │   ├── NUMBER <- INTEGER
  // │   ├── TIMESTAMP_WITH_TIMEZONE <- TIMESTAMP_WITHOUT_TIMEZONE <- DATE
  // │   └── TIME_WITH_TIMEZONE <- TIME_WITHOUT_TIMEZONE
  // ├── OBJECT
  // ├── ARRAY
  // └── MULTI
  private val PARENT: Map<FieldType, FieldType> =
    mapOf(
      FieldType.INTEGER to FieldType.NUMBER,
      FieldType.NUMBER to FieldType.STRING,
      FieldType.BOOLEAN to FieldType.STRING,
      FieldType.DATE to FieldType.TIMESTAMP_WITHOUT_TIMEZONE,
      FieldType.TIMESTAMP_WITHOUT_TIMEZONE to FieldType.TIMESTAMP_WITH_TIMEZONE,
      FieldType.TIMESTAMP_WITH_TIMEZONE to FieldType.STRING,
      FieldType.TIME_WITHOUT_TIMEZONE to FieldType.TIME_WITH_TIMEZONE,
      FieldType.TIME_WITH_TIMEZONE to FieldType.STRING,
      FieldType.STRING to FieldType.UNKNOWN,
      FieldType.OBJECT to FieldType.UNKNOWN,
      FieldType.ARRAY to FieldType.UNKNOWN,
      FieldType.MULTI to FieldType.UNKNOWN,
    )

  private val fieldGenerator = FieldGenerator()

  data class WidenedColumn(
    val name: String,
    val savedType: FieldType,
    val sourceType: FieldType,
  )

  /**
   * @property widenedColumns the columns this merge wrote a new `airbyte_source_type` to.
   */
  data class MergeResult(
    val jsonSchema: JsonNode,
    val widenedColumns: List<WidenedColumn>,
  )

  /**
   * The closest type both [oldType] and [newType] fit under.
   */
  fun widen(
    oldType: FieldType,
    newType: FieldType,
  ): FieldType {
    val oldChain = selfAndParents(oldType)
    return selfAndParents(newType).first { it in oldChain }
  }

  /**
   * The value written to `airbyte_source_type` for [type].
   */
  fun sourceTypeName(type: FieldType): String = type.name.lowercase()

  /**
   * Builds the schema to save for a stream from its newly discovered schema, column by column. A top-level column
   * whose type changed keeps a supertype of its saved and discovered types. Every other column is taken from
   * [discoveredSchema], as when the whole stream is copied.
   *
   * @param keyColumns top-level primary key and cursor columns. A type change on one of them is breaking, so they
   * are never widened.
   */
  fun mergeStreamSchema(
    savedSchema: JsonNode,
    discoveredSchema: JsonNode,
    keyColumns: Set<String>,
  ): MergeResult {
    val merged = discoveredSchema.deepCopy<JsonNode>()
    val mergedProperties = merged.get(PROPERTIES)
    if (mergedProperties !is ObjectNode) {
      return MergeResult(merged, emptyList())
    }

    val savedTypes = fieldGenerator.getFieldsFromSchema(savedSchema).associate { it.name to it.type }
    val discoveredTypes = fieldGenerator.getFieldsFromSchema(discoveredSchema).associate { it.name to it.type }
    val widenedColumns = mutableListOf<WidenedColumn>()

    discoveredTypes.forEach { (name, newType) ->
      val oldType = savedTypes[name] ?: return@forEach
      val savedColumn = savedSchema.get(PROPERTIES).get(name)
      val newColumn = mergeColumn(name, savedColumn, mergedProperties.get(name), oldType, newType, keyColumns)
      mergedProperties.set<JsonNode>(name, newColumn)
      if (newColumn.has(AIRBYTE_SOURCE_TYPE) && newColumn != savedColumn) {
        widenedColumns.add(WidenedColumn(name, oldType, newType))
      }
    }

    return MergeResult(merged, widenedColumns)
  }

  private fun mergeColumn(
    name: String,
    savedColumn: JsonNode,
    discoveredColumn: JsonNode,
    oldType: FieldType,
    newType: FieldType,
    keyColumns: Set<String>,
  ): JsonNode {
    // The database type hasn't changed since the column was widened.
    if (savedColumn.get(AIRBYTE_SOURCE_TYPE)?.asText() == sourceTypeName(newType)) {
      return savedColumn.deepCopy()
    }

    // UNKNOWN here means discover returned a schema we can't classify, not a real type change.
    if (oldType == newType || newType == FieldType.UNKNOWN || name in keyColumns) {
      return discoveredColumn
    }

    val supertype = widen(oldType, newType)
    if (supertype == newType) {
      return discoveredColumn
    }

    val column: ObjectNode =
      if (supertype == oldType) {
        savedColumn.deepCopy()
      } else {
        Jsons.jsonNode(supertype.toMap()) as ObjectNode
      }
    column.put(AIRBYTE_SOURCE_TYPE, sourceTypeName(newType))
    return column
  }

  private fun selfAndParents(type: FieldType): List<FieldType> = generateSequence(type) { PARENT[it] }.toList()
}
