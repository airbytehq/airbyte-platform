/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeclarativeSourceManifestInjectorTest {

  private static final JsonNode A_SPEC;
  private static final JsonNode A_MANIFEST;
  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final URI DOCUMENTATION_URL = URI.create("https://documentation-url.com");

  static {
    try {
      A_SPEC = new ObjectMapper().readTree(
          "{\"connectionSpecification\":{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[],\"properties\":{},\"additionalProperties\":true}}\n");
      A_MANIFEST = new ObjectMapper().readTree("{\"manifest_key\": \"manifest value\", \"version\": \"1.0.0\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private DeclarativeSourceManifestInjector injector;

  @BeforeEach
  void setUp() {
    injector = new DeclarativeSourceManifestInjector();
  }

  @Test
  void whenAddInjectedDeclarativeManifestThenJsonHasInjectedDeclarativeManifestProperty() throws JsonProcessingException {
    final JsonNode spec = A_SPEC.deepCopy();
    injector.addInjectedDeclarativeManifest(spec);
    assertEquals(
        new ObjectMapper()
            .readTree("{\"__injected_declarative_manifest\": {\"type\": \"object\", \"additionalProperties\": true, \"airbyte_hidden\": true}}"),
        spec.path("connectionSpecification").path("properties"));
  }

  @Test
  void whenCreateConfigInjectionThenReturnConfigInjection() {
    final ActorDefinitionConfigInjection configInjection = injector.createConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST);
    assertEquals(new ActorDefinitionConfigInjection().withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath("__injected_declarative_manifest").withJsonToInject(A_MANIFEST), configInjection);
  }

  @Test
  void whenAdaptDeclarativeManifestThenReturnConnectorSpecification() {
    final ConnectorSpecification connectorSpecification = injector.createDeclarativeManifestConnectorSpecification(A_SPEC);
    assertEquals(new ConnectorSpecification()
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
        .withProtocolVersion(DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
        .withDocumentationUrl(URI.create(""))
        .withConnectionSpecification(A_SPEC.get("connectionSpecification")), connectorSpecification);
  }

  @Test
  void givenDocumentationUrlWhenAdaptDeclarativeManifestThenReturnConnectorSpecificationHasDocumentationUrl() {
    final JsonNode spec = givenSpecWithDocumentationUrl(DOCUMENTATION_URL);
    final ConnectorSpecification connectorSpecification = injector.createDeclarativeManifestConnectorSpecification(spec);
    assertEquals(DOCUMENTATION_URL, connectorSpecification.getDocumentationUrl());
  }

  @Test
  void testGetCdkVersion() {
    assertEquals(new Version("1.0.0"), injector.getCdkVersion(A_MANIFEST));
  }

  private JsonNode givenSpecWithDocumentationUrl(final URI documentationUrl) {
    final JsonNode spec = A_SPEC.deepCopy();
    ((ObjectNode) spec).put("documentationUrl", documentationUrl.toString());
    return spec;
  }

}
