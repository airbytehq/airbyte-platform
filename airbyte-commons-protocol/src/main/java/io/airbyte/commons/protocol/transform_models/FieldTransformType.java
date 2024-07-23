/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transform_models;

/**
 * Types of transformations possible for a field.
 */
public enum FieldTransformType {
  ADD_FIELD,
  REMOVE_FIELD,
  UPDATE_FIELD_SCHEMA
}
