/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.protocol.models.AirbyteProtocolSchema
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonSchemaValidator.Companion.getSchema
import java.util.function.Predicate

/**
 * Verify that the provided JsonNode is a valid AirbyteMessage. Any AirbyteMessage type is allowed
 * (e.g. Record, State, Log, etc).
 */
class AirbyteProtocolPredicate : Predicate<JsonNode?> {
  private val jsonSchemaValidator = JsonSchemaValidator()

  init {
    val schema = getSchema(AirbyteProtocolSchema.PROTOCOL.file, "AirbyteMessage")
    jsonSchemaValidator.initializeSchemaValidator(PROTOCOL_SCHEMA_NAME, schema)
  }

  override fun test(s: JsonNode?): Boolean = jsonSchemaValidator.testInitializedSchema(PROTOCOL_SCHEMA_NAME, s)

  companion object {
    private const val PROTOCOL_SCHEMA_NAME = "protocol schema"
  }
}
