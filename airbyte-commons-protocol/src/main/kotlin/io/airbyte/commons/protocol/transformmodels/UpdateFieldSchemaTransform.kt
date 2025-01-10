/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.protocol.transformmodels

import com.fasterxml.jackson.databind.JsonNode

/**
 * Represents the update of a field.
 */
data class UpdateFieldSchemaTransform(
  val oldSchema: JsonNode?,
  val newSchema: JsonNode?,
)
