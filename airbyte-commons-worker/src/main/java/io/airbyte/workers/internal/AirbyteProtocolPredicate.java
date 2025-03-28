/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.protocol.models.AirbyteProtocolSchema;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.util.function.Predicate;

/**
 * Verify that the provided JsonNode is a valid AirbyteMessage. Any AirbyteMessage type is allowed
 * (e.g. Record, State, Log, etc).
 */
public class AirbyteProtocolPredicate implements Predicate<JsonNode> {

  private static final String PROTOCOL_SCHEMA_NAME = "protocol schema";
  private final JsonSchemaValidator jsonSchemaValidator;

  public AirbyteProtocolPredicate() {
    jsonSchemaValidator = new JsonSchemaValidator();
    final JsonNode schema = JsonSchemaValidator.getSchema(AirbyteProtocolSchema.PROTOCOL.getFile(), "AirbyteMessage");
    jsonSchemaValidator.initializeSchemaValidator(PROTOCOL_SCHEMA_NAME, schema);
  }

  @Override
  public boolean test(final JsonNode s) {
    return jsonSchemaValidator.testInitializedSchema(PROTOCOL_SCHEMA_NAME, s);
  }

}
