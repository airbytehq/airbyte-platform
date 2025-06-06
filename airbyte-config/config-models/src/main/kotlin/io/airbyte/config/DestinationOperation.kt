/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.enums.Enums
import io.airbyte.config.helpers.FieldGenerator

data class DestinationOperation(
  val objectName: String,
  val syncMode: DestinationSyncMode,
  val jsonSchema: JsonNode,
  val matchingKeys: List<List<String>>? = null,
) {
  fun getFields(): List<Field> = FieldGenerator().getFieldsFromSchema(jsonSchema)
}

fun DestinationOperation.toProtocol(): io.airbyte.protocol.models.v0.DestinationOperation =
  io.airbyte.protocol.models.v0
    .DestinationOperation()
    .withObjectName(objectName)
    .withSyncMode(Enums.convertTo(syncMode, io.airbyte.protocol.models.v0.DestinationSyncMode::class.java))
    .withMatchingKeys(matchingKeys)
    .withJsonSchema(jsonSchema)

fun io.airbyte.protocol.models.v0.DestinationOperation.toModel(): DestinationOperation =
  DestinationOperation(
    objectName = objectName,
    syncMode = Enums.convertTo(syncMode, DestinationSyncMode::class.java),
    jsonSchema = jsonSchema,
    matchingKeys = matchingKeys,
  )
