/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector.INJECTED_COMPONENT_FILE_CHECKSUMS_KEY;
import static io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector.INJECTED_COMPONENT_FILE_KEY;
import static io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector.INJECTED_DECLARATIVE_MANIFEST_KEY;
import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("LineLength")
class DeclarativeSourceManifestInjectorTest {

  private static final JsonNode A_SPEC;
  private static final JsonNode A_MANIFEST;
  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final String A_COMPONENT_FILE =
      "from dataclasses import dataclass\\n\\nfrom airbyte_cdk.sources.declarative.transformations import AddFields\\n\\n\\n@dataclass\\nclass OverrideAddFields(AddFields):\\n    pass";
  private static final String A_COMPONENT_FILE_MD5_HASH = "cc93b2d066f94e041da68ecd251396f3";
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
        new ObjectMapper().readTree("""
                                    {
                                      "__injected_declarative_manifest": {
                                        "type": "object",
                                        "additionalProperties": true,
                                        "airbyte_hidden": true
                                      },
                                      "__injected_components_py": {
                                        "type": "string",
                                        "airbyte_hidden": true
                                      },
                                      "__injected_components_py_checksums": {
                                        "type": "object",
                                        "additionalProperties": true,
                                        "airbyte_hidden": true
                                      }
                                    }"""),
        spec.path("connectionSpecification").path("properties"));
  }

  @Test
  void whenCreateConfigInjectionThenReturnManifestConfigInjection() {
    final ActorDefinitionConfigInjection configInjection = injector.createManifestConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST);
    assertEquals(new ActorDefinitionConfigInjection().withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(INJECTED_DECLARATIVE_MANIFEST_KEY).withJsonToInject(A_MANIFEST), configInjection);
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
  void whenAddInjectedCustomComponentsMD5HashIsCalculated() {
    final ActorDefinitionConfigInjection checkSumInjection = injector.createComponentFileChecksumsInjection(A_SOURCE_DEFINITION_ID, A_COMPONENT_FILE);
    final String actualMd5Hash = checkSumInjection.getJsonToInject().get("md5").asText();

    assertEquals(A_COMPONENT_FILE_MD5_HASH, actualMd5Hash);
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

  @Test
  void whenGetManifestConnectorInjectionsWithNoCustomCodeThenReturnOnlyManifestInjection() throws IOException {
    final var injections = injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, null);

    assertEquals(1, injections.size());
    assertEquals(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
        injections.get(0));
  }

  @Test
  void whenGetManifestConnectorInjectionsWithCustomCodeThenReturnAllInjections() throws IOException {
    final var injections = injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, A_COMPONENT_FILE);

    assertEquals(3, injections.size());

    // Verify manifest injection
    assertEquals(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
        injections.get(0));

    // Verify component file injection
    final var expectedComponentJson = new ObjectMapper().readValue("\"" + A_COMPONENT_FILE.replace("\\n", "\\\\n") + "\"", JsonNode.class);
    assertEquals(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(INJECTED_COMPONENT_FILE_KEY)
        .withJsonToInject(expectedComponentJson),
        injections.get(1));

    // Verify checksum injection
    assertEquals(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(INJECTED_COMPONENT_FILE_CHECKSUMS_KEY)
        .withJsonToInject(new ObjectMapper().readTree("{\"md5\":\"" + A_COMPONENT_FILE_MD5_HASH + "\"}"))
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID),
        injections.get(2));
  }

  @Test
  void whenGetManifestConnectorInjectionsWithEmptyCustomCodeThenReturnOnlyManifestInjection() throws IOException {
    final var injections = injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, "");

    assertEquals(1, injections.size());
    assertEquals(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
        injections.get(0));
  }

  private JsonNode givenSpecWithDocumentationUrl(final URI documentationUrl) {
    final JsonNode spec = A_SPEC.deepCopy();
    ((ObjectNode) spec).put("documentationUrl", documentationUrl.toString());
    return spec;
  }

}
