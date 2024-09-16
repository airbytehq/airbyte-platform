package io.airbyte.mappers.transformations

import io.airbyte.config.Field
import io.airbyte.config.FieldType

/**
 * Subset of Stream configuration that is relevant to mappers.
 */
class SlimStream(fields: List<Field>, cursor: List<String>? = null, primaryKey: List<List<String>>? = null) {
  private val _fields: MutableList<Field> = fields.toMutableList()
  private val _cursor: MutableList<String>? = cursor?.toMutableList()
  private val _primaryKey: MutableList<MutableList<String>>? = primaryKey?.map { it.toMutableList() }?.toMutableList()

  val fields: List<Field>
    get() = _fields.toList()

  val cursor: List<String>?
    get() = _cursor?.toList()

  val primaryKey: List<List<String>>?
    get() = _primaryKey?.map { it.toList() }?.toList()

  fun deepCopy(
    fields: List<Field>? = null,
    cursor: List<String>? = null,
    primaryKey: List<List<String>>? = null,
  ): SlimStream =
    // This relies on the fact that the constructor is copying into a mutable list
    SlimStream(
      fields = fields ?: _fields.map { it.copy() },
      cursor = cursor ?: _cursor,
      primaryKey = primaryKey ?: _primaryKey,
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
    if (_fields.any { it.name == newName }) {
      throw IllegalStateException("Field $newName already exists in stream fields")
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
      throw IllegalStateException("Field $oldName not found in stream fields")
    }

    _cursor?.apply {
      if (this == listOf(oldName)) {
        this[0] = newName
      }
    }

    _primaryKey?.apply {
      this.replaceAll {
        if (it == listOf(oldName)) mutableListOf(newName) else it
      }
    }
  }
}
