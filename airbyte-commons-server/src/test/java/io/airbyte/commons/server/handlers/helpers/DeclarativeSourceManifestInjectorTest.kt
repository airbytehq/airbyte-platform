/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.URI
import java.util.UUID

internal class DeclarativeSourceManifestInjectorTest {
  private lateinit var injector: DeclarativeSourceManifestInjector

  @BeforeEach
  fun setUp() {
    injector = DeclarativeSourceManifestInjector()
  }

  @Test
  @Throws(JsonProcessingException::class)
  fun whenAddInjectedDeclarativeManifestThenJsonHasInjectedDeclarativeManifestProperty() {
    val spec: JsonNode = A_SPEC.deepCopy()
    injector.addInjectedDeclarativeManifest(spec)
    Assertions.assertEquals(
      ObjectMapper().readTree(
        """
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
        }
        """.trimIndent(),
      ),
      spec.path("connectionSpecification").path("properties"),
    )
  }

  @Test
  fun whenCreateConfigInjectionThenReturnManifestConfigInjection() {
    val configInjection = injector.createManifestConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST)
    Assertions.assertEquals(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(DeclarativeSourceManifestInjector.INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
      configInjection,
    )
  }

  @Test
  fun whenAdaptDeclarativeManifestThenReturnConnectorSpecification() {
    val connectorSpecification = injector.createDeclarativeManifestConnectorSpecification(A_SPEC)
    Assertions.assertEquals(
      ConnectorSpecification()
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
        .withProtocolVersion(Version("0.2.0").serialize())
        .withDocumentationUrl(URI.create(""))
        .withConnectionSpecification(A_SPEC.get("connectionSpecification")),
      connectorSpecification,
    )
  }

  @Test
  fun whenAddInjectedCustomComponentsMD5HashIsCalculated() {
    val checkSumInjection = injector.createComponentFileChecksumsInjection(A_SOURCE_DEFINITION_ID, A_COMPONENT_FILE)
    val actualMd5Hash = checkSumInjection.getJsonToInject().get("md5").asText()

    Assertions.assertEquals(A_COMPONENT_FILE_MD5_HASH, actualMd5Hash)
  }

  @Test
  fun givenDocumentationUrlWhenAdaptDeclarativeManifestThenReturnConnectorSpecificationHasDocumentationUrl() {
    val spec = givenSpecWithDocumentationUrl(DOCUMENTATION_URL)
    val connectorSpecification = injector.createDeclarativeManifestConnectorSpecification(spec)
    Assertions.assertEquals(DOCUMENTATION_URL, connectorSpecification.getDocumentationUrl())
  }

  @Test
  fun testGetCdkVersion() {
    Assertions.assertEquals(Version("1.0.0"), injector.getCdkVersion(A_MANIFEST))
  }

  @Test
  @Throws(IOException::class)
  fun whenGetManifestConnectorInjectionsWithNoCustomCodeThenReturnOnlyManifestInjection() {
    val injections: List<ActorDefinitionConfigInjection> =
      injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, null)

    Assertions.assertEquals(1, injections.size)
    Assertions.assertEquals(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(DeclarativeSourceManifestInjector.INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
      injections[0],
    )
  }

  @Test
  @Throws(IOException::class)
  fun whenGetManifestConnectorInjectionsWithCustomCodeThenReturnAllInjections() {
    val injections: List<ActorDefinitionConfigInjection> =
      injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, A_COMPONENT_FILE)

    Assertions.assertEquals(3, injections.size)

    // Verify manifest injection
    Assertions.assertEquals(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(DeclarativeSourceManifestInjector.INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
      injections[0],
    )

    // Verify component file injection
    val expectedComponentJson = ObjectMapper().readValue("\"" + A_COMPONENT_FILE.replace("\\n", "\\\\n") + "\"", JsonNode::class.java)
    Assertions.assertEquals(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(DeclarativeSourceManifestInjector.INJECTED_COMPONENT_FILE_KEY)
        .withJsonToInject(expectedComponentJson),
      injections[1],
    )

    // Verify checksum injection
    Assertions.assertEquals(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(DeclarativeSourceManifestInjector.INJECTED_COMPONENT_FILE_CHECKSUMS_KEY)
        .withJsonToInject(ObjectMapper().readTree("{\"md5\":\"" + A_COMPONENT_FILE_MD5_HASH + "\"}"))
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID),
      injections[2],
    )
  }

  @Test
  @Throws(IOException::class)
  fun whenGetManifestConnectorInjectionsWithEmptyCustomCodeThenReturnOnlyManifestInjection() {
    val injections: List<ActorDefinitionConfigInjection> =
      injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, "")

    Assertions.assertEquals(1, injections.size)
    Assertions.assertEquals(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withInjectionPath(DeclarativeSourceManifestInjector.INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST),
      injections[0],
    )
  }

  private fun givenSpecWithDocumentationUrl(documentationUrl: URI): JsonNode {
    val spec: JsonNode = A_SPEC.deepCopy()
    (spec as ObjectNode).put("documentationUrl", documentationUrl.toString())
    return spec
  }

  companion object {
    private val A_SPEC: JsonNode
    private val A_MANIFEST: JsonNode
    private val A_SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val A_COMPONENT_FILE =
      "from dataclasses import dataclass\\n\\nfrom airbyte_cdk.sources.declarative.transformations import AddFields\\n\\n\\n@dataclass\\nclass OverrideAddFields(AddFields):\\n    pass"
    private const val A_COMPONENT_FILE_MD5_HASH = "cc93b2d066f94e041da68ecd251396f3"
    private val DOCUMENTATION_URL: URI = URI.create("https://documentation-url.com")

    init {
      try {
        A_SPEC =
          ObjectMapper().readTree(
            "{\"connectionSpecification\":{\"\$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[],\"properties\":{},\"additionalProperties\":true}}\n",
          )
        A_MANIFEST = ObjectMapper().readTree("{\"manifest_key\": \"manifest value\", \"version\": \"1.0.0\"}")
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }
  }
}
