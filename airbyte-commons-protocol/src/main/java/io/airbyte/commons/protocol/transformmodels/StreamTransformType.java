/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels;

/**
 * Types of transformations possible for a stream.
 */
public enum StreamTransformType {
  ADD_STREAM,
  REMOVE_STREAM,
  UPDATE_STREAM
}
