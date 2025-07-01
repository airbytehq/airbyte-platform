/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

import com.fasterxml.jackson.databind.JsonNode
import java.util.Objects

/**
 * Represents the diff between two fields.
 */
data class FieldTransform(
  @JvmField val transformType: FieldTransformType,
  @JvmField val fieldName: List<String>,
  @JvmField val addFieldTransform: AddFieldTransform?,
  @JvmField val removeFieldTransform: RemoveFieldTransform?,
  @JvmField val updateFieldTransform: UpdateFieldSchemaTransform?,
  private val breaking: Boolean,
) {
  fun breaking(): Boolean = breaking

  override fun equals(o: Any?): Boolean {
    if (o == null || javaClass != o.javaClass) {
      return false
    }
    val that = o as FieldTransform
    return breaking == that.breaking &&
      transformType == that.transformType &&
      fieldName == that.fieldName &&
      addFieldTransform == that.addFieldTransform &&
      removeFieldTransform == that.removeFieldTransform &&
      updateFieldTransform == that.updateFieldTransform
  }

  override fun hashCode(): Int = Objects.hash(transformType, fieldName, addFieldTransform, removeFieldTransform, updateFieldTransform, breaking)

  override fun toString(): String =
    (
      "FieldTransform{" +
        "transformType=" + transformType +
        ", fieldName=" + fieldName +
        ", addFieldTransform=" + addFieldTransform +
        ", removeFieldTransform=" + removeFieldTransform +
        ", updateFieldTransform=" + updateFieldTransform +
        ", breaking=" + breaking +
        '}'
    )

  companion object {
    @JvmStatic
    fun createAddFieldTransform(
      fieldName: List<String>,
      schema: JsonNode,
    ): FieldTransform = createAddFieldTransform(fieldName, AddFieldTransform(schema))

    @JvmStatic
    fun createAddFieldTransform(
      fieldName: List<String>,
      addFieldTransform: AddFieldTransform,
    ): FieldTransform = FieldTransform(FieldTransformType.ADD_FIELD, fieldName, addFieldTransform, null, null, false)

    @JvmStatic
    fun createRemoveFieldTransform(
      fieldName: List<String>,
      schema: JsonNode,
      breaking: Boolean,
    ): FieldTransform = createRemoveFieldTransform(fieldName, RemoveFieldTransform(fieldName, schema), breaking)

    @JvmStatic
    fun createRemoveFieldTransform(
      fieldName: List<String>,
      removeFieldTransform: RemoveFieldTransform?,
      breaking: Boolean,
    ): FieldTransform = FieldTransform(FieldTransformType.REMOVE_FIELD, fieldName, null, removeFieldTransform, null, breaking)

    @JvmStatic
    fun createUpdateFieldTransform(
      fieldName: List<String>,
      updateFieldTransform: UpdateFieldSchemaTransform?,
    ): FieldTransform = FieldTransform(FieldTransformType.UPDATE_FIELD_SCHEMA, fieldName, null, null, updateFieldTransform, false)
  }
}
