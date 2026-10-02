/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.net.URI
import java.util.UUID

internal class DeclarativeSourceManifestInjectorTest {
  private lateinit var injector: DeclarativeSourceManifestInjector

  @BeforeEach
  fun setUp() {
    injector = DeclarativeSourceManifestInjector()
  }

  @Test
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

  @ParameterizedTest
  @MethodSource("componentFilesWithCdkMd5")
  fun whenGetManifestConnectorInjectionsThenChecksumMatchesInjectedComponentFile(
    componentFile: String,
    expectedMd5: String,
  ) {
    val injections = injector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, A_MANIFEST, componentFile)

    Assertions.assertEquals(componentFile, injections[1].jsonToInject.asText())
    Assertions.assertEquals(expectedMd5, injections[2].jsonToInject.get("md5").asText())
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
    val expectedComponentJson = TextNode.valueOf(A_COMPONENT_FILE)
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
      "from dataclasses import dataclass\n\nfrom airbyte_cdk.sources.declarative.transformations import AddFields\n\n\n@dataclass\nclass OverrideAddFields(AddFields):\n    pass"
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

    // Expected values from airbyte_cdk custom_code_compiler._hash_text on the same text.
    @JvmStatic
    private fun componentFilesWithCdkMd5() =
      listOf(
        Arguments.of("NAME = '\\u4e2d'\n", "44713eeb6b1b11cc62935db229e9068c"),
        Arguments.of("NAME = '中文'\n", "c29fc23ad2dc698af4a8ab91fea362ad"),
        Arguments.of("E = '\uD83D\uDE00'\n", "db7e1b95876c6b8e6a7aef339a387dc1"),
        Arguments.of("x = 1\r\ny = 2\r\n", "da5d665f762851a81554363c76eae0de"),
        Arguments.of("s = 'a\\nb'\n", "86606756b46bc4c0e949efd607e7e18b"),
        Arguments.of("s = '\\\\'\n", "f09ae8c8fa968eb3b01936c7c8438bb9"),
        Arguments.of("P = re.compile(r'\\d+')\n", "5cfbc7a513654f0ae158f6e60ee80bb9"),
        Arguments.of("P = r'C:\\users\\x'\n", "b890b80bae5b0cac4d86144747fa5d06"),
        Arguments.of("x = 1 + \\\n    2\n", "1b4794c7289b9eabf010c30aa2e91f98"),
      )
  }
}
