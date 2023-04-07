/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utilities for use by the handler functions.
 */
public class HandlerHelper {

  public JsonNode getManifestJson(final Object manifest) {
    final ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.convertValue(manifest, JsonNode.class);
  }

}
