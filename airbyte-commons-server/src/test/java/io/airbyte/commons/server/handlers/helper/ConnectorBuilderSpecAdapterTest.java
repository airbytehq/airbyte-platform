/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.server.handlers.helpers.ConnectorBuilderSpecAdapter;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderSpecAdapterTest {

  private static final String SPEC =
      "{\"connectionSpecification\":{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[],\"properties\":{},\"additionalProperties\":true},\"documentationUrl\":\"https://example.org\"}\n";
  private static final String CONNECTION_SPECIFICATION_WITH_ADDED_PROPERTIES =
      "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[],\"properties\":{\"__injected_declarative_manifest\":{\"type\":\"object\",\"additionalProperties\":true}},\"additionalProperties\":true}\n";
  private ConnectorBuilderSpecAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter = new ConnectorBuilderSpecAdapter();
  }

  @Test
  void whenAdaptThenReturnConnectorSpecificationWithAddedProperties() throws JsonProcessingException {
    final JsonNode spec = new ObjectMapper().readTree(SPEC);

    final ConnectorSpecification adaptedSpec = adapter.adapt(spec);

    assertEquals(new ConnectorSpecification()
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
        .withProtocolVersion("0.2.0")
        .withDocumentationUrl(URI.create("https://example.org"))
        .withConnectionSpecification(new ObjectMapper().readTree(CONNECTION_SPECIFICATION_WITH_ADDED_PROPERTIES)), adaptedSpec);
  }

  @Test
  void givenDocumentationUrlNotProvidedWhenAdaptThenDocumentationUrlIsEmptyString() throws JsonProcessingException {
    final JsonNode spec = new ObjectMapper().readTree(
        "{\"connectionSpecification\":{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[],\"properties\":{},\"additionalProperties\":true}}\n");
    final ConnectorSpecification adaptedSpec = adapter.adapt(spec);
    assertEquals(URI.create(""), adaptedSpec.getDocumentationUrl());
  }

}
