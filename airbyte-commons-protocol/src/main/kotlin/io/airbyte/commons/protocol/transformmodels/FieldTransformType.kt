/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

/**
 * Types of transformations possible for a field.
 */
enum class FieldTransformType {
  ADD_FIELD,
  REMOVE_FIELD,
  UPDATE_FIELD_SCHEMA,
}
