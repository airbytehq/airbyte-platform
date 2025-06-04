/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.helpers.FieldGenerator

data class DestinationOperation(
  val objectName: String,
  val syncMode: DestinationSyncMode,
  val jsonSchema: JsonNode,
  val matchingKeys: List<List<String>>? = null,
) {
  fun getFields(): List<Field> = FieldGenerator().getFieldsFromSchema(jsonSchema)
}
