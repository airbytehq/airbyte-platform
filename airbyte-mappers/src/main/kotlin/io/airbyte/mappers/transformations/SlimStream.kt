/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import io.airbyte.config.Field
import io.airbyte.config.FieldType

/**
 * Subset of Stream configuration that is relevant to mappers.
 */
class SlimStream(
  fields: List<Field>,
  cursor: List<String>? = null,
  primaryKey: List<List<String>>? = null,
  sourceDefaultCursor: List<String>? = null,
  sourceDefinedPrimaryKey: List<List<String>>? = null,
) {
  private val _fields: MutableList<Field> = fields.toMutableList()
  private var _cursor: MutableList<String>? = cursor?.toMutableList()
  private var _primaryKey: MutableList<MutableList<String>>? = primaryKey?.map { it.toMutableList() }?.toMutableList()
  private var _sourceDefaultCursor: MutableList<String>? = sourceDefaultCursor?.toMutableList()
  private var _sourceDefinedPrimaryKey: MutableList<MutableList<String>>? = sourceDefinedPrimaryKey?.map { it.toMutableList() }?.toMutableList()

  val fields: List<Field>
    get() = _fields.toList()

  val cursor: List<String>?
    get() = _cursor?.toList()

  val primaryKey: List<List<String>>?
    get() = _primaryKey?.map { it.toList() }?.toList()

  val sourceDefaultCursor: List<String>?
    get() = _sourceDefaultCursor?.toList()

  val sourceDefinedPrimaryKey: List<List<String>>?
    get() = _sourceDefinedPrimaryKey?.map { it.toList() }?.toList()

  fun deepCopy(
    fields: List<Field>? = null,
    cursor: List<String>? = null,
    primaryKey: List<List<String>>? = null,
    sourceDefaultCursor: List<String>? = null,
    sourceDefinedPrimaryKey: List<List<String>>? = null,
  ): SlimStream =
    // This relies on the fact that the constructor is copying into a mutable list
    SlimStream(
      fields = fields ?: _fields.map { it.copy() },
      cursor = cursor ?: _cursor,
      primaryKey = primaryKey ?: _primaryKey,
      sourceDefaultCursor = sourceDefaultCursor ?: _sourceDefaultCursor,
      sourceDefinedPrimaryKey = sourceDefinedPrimaryKey ?: _sourceDefinedPrimaryKey,
    )

  /**
   * Redefine an existing field. Other configuration of the stream such as cursor or primary key will be updated as well if
   * they were referencing the oldName.
   * Nested fields are not supported at this moment.
   *
   * @param oldName the name of the field to redefine
   * @param newName the new name of the field
   * @param newType, optionally, provide a new type for the field being redefined
   */
  fun redefineField(
    oldName: String,
    newName: String,
    newType: FieldType? = null,
  ) {
    if (oldName != newName && _fields.any { it.name == newName }) {
      throw MapperException(
        type = DestinationCatalogGenerator.MapperErrorType.FIELD_ALREADY_EXISTS,
        message = "Field $newName already exists in stream fields",
      )
    }

    var match = 0
    _fields.replaceAll {
      if (it.name == oldName) {
        match++
        it.copy(name = newName, type = newType ?: it.type)
      } else {
        it
      }
    }
    if (match == 0) {
      throw MapperException(type = DestinationCatalogGenerator.MapperErrorType.FIELD_NOT_FOUND, message = "Field $oldName not found in stream fields")
    }

    _cursor?.apply { renameInSimpleList(this, oldName, newName) }
    _primaryKey?.apply { renameInNestedList(this, oldName, newName) }
    _sourceDefaultCursor?.apply { renameInSimpleList(this, oldName, newName) }
    _sourceDefinedPrimaryKey?.apply { renameInNestedList(this, oldName, newName) }
  }

  fun removeField(targetName: String) {
    val result = _fields.removeAll { it.name == targetName }
    if (!result) {
      throw MapperException(
        type = DestinationCatalogGenerator.MapperErrorType.FIELD_NOT_FOUND,
        message = "Field $targetName not found in stream fields",
      )
    }

    _cursor = removeFromSimpleList(_cursor, targetName)
    _primaryKey = removeFromNestedList(_primaryKey, targetName)
    _sourceDefaultCursor = removeFromSimpleList(_sourceDefaultCursor, targetName)
    _sourceDefinedPrimaryKey = removeFromNestedList(_sourceDefinedPrimaryKey, targetName)
  }

  private fun removeFromSimpleList(
    list: MutableList<String>?,
    targetName: String,
  ): MutableList<String>? {
    list?.removeAll { it == targetName }
    return if (list?.isNotEmpty() == true) list else null
  }

  private fun removeFromNestedList(
    list: MutableList<MutableList<String>>?,
    targetName: String,
  ): MutableList<MutableList<String>>? {
    val targetListToRemove = listOf(targetName)
    list?.removeAll { it == targetListToRemove }
    return if (list?.isNotEmpty() == true) list else null
  }

  private fun renameInSimpleList(
    list: MutableList<String>,
    oldName: String,
    newName: String,
  ) {
    list.apply {
      if (this == listOf(oldName)) {
        this[0] = newName
      }
    }
  }

  private fun renameInNestedList(
    list: MutableList<MutableList<String>>,
    oldName: String,
    newName: String,
  ) {
    list.apply {
      this.replaceAll {
        if (it == listOf(oldName)) mutableListOf(newName) else it
      }
    }
  }
}
