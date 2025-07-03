/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

import com.fasterxml.jackson.databind.JsonNode

/**
 * Represents the removal of a field to an {@link io.airbyte.protocol.models.AirbyteStream}.
 */
data class RemoveFieldTransform(
  val fieldName: List<String>,
  val schema: JsonNode,
)
