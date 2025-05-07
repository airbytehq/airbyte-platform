/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.utils

import io.airbyte.connectorbuilder.exceptions.CircularReferenceException
import io.airbyte.connectorbuilder.exceptions.ManifestParserException
import io.airbyte.connectorbuilder.exceptions.UndefinedReferenceException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ManifestParserTest {
  @Test
  fun `ensure processManifestYaml works`() {
    val yaml =
      """
            |allowedHosts:
            |  - host1
            |  - host2
            |streams:
            |  - stream1
            |  - stream2
            |manifestYamlString: "string"
      """.trimMargin()

    val expected =
      mapOf(
        "allowedHosts" to listOf("host1", "host2"),
        "streams" to listOf("stream1", "stream2"),
        "manifestYamlString" to "string",
      )

    val result = ManifestParser(yaml).manifestMap
    assertEquals(expected, result)
  }

  @Test
  fun `ensure processManifestYaml follows $ref appropriately`() {
    val yaml =
      """
            |allowedHosts:
            |  - host1
            |  - host2
            |streams:
            |  - stream1
            |  - stream2
            |manifestYamlString: "string"
            |allowedHostsRef:
            ${"\$ref"}: "#/allowedHosts"
            |streamsNonRef: "#/streams"
      """.trimMargin()

    val expected =
      mapOf(
        "allowedHosts" to listOf("host1", "host2"),
        "streams" to listOf("stream1", "stream2"),
        "manifestYamlString" to "string",
        "allowedHostsRef" to listOf("host1", "host2"),
        "streamsNonRef" to listOf("stream1", "stream2"),
      )

    val result = ManifestParser(yaml).manifestMap
    assertEquals(expected, result)
  }

  @Test
  fun `ensure processManifestYaml follows nested $ref`() {
    val yaml =
      """
            |definitions:
            |   streams:
            |     cards:
            |       name: CARDS
            |     collections:
            |       name: COLLECTIONS
            |     
            |streams:
            |  - stream1
            |  - stream2
            |  - ${"\$ref"}: "#/definitions/streams/cards"
            |  - ${"\$ref"}: "#/definitions/streams/collections"
            |manifestYamlString: "string"
      """.trimMargin()

    val expected =
      mapOf(
        "definitions" to
          mapOf(
            "streams" to
              mapOf(
                "cards" to mapOf("name" to "CARDS"),
                "collections" to mapOf("name" to "COLLECTIONS"),
              ),
          ),
        "streams" to listOf("stream1", "stream2", mapOf("name" to "CARDS"), mapOf("name" to "COLLECTIONS")),
        "manifestYamlString" to "string",
      )

    val result = ManifestParser(yaml).manifestMap
    assertEquals(expected, result)
  }

  @Test
  fun `ensure processManifestYaml throws error on $ref not found`() {
    val yaml =
      """
            |allowedHosts:
            |  - host1
            |  - host2
            |streams:
            |  - stream1
            |  - stream2
            |manifestYamlString: "string"
            |allowedHostsRef:
            ${"\$ref"}: "#/allowedHostsNotFound"
      """.trimMargin()

    assertThrows(UndefinedReferenceException::class.java) {
      ManifestParser(yaml).manifestMap
    }
  }

  @Test
  fun `ensure processManifestYaml throws error on circular`() {
    val yaml =
      """
            |definitions:
            |   streams:
            |     cards:
            |       name: CARDS
            |     collections:
            |       ${"\$ref"}: "#/streams"
            |     
            |streams:
            |  - stream1
            |  - stream2
            |  - ${"\$ref"}: "#/definitions/streams/cards"
            |  - ${"\$ref"}: "#/definitions/streams/collections"
            |manifestYamlString: "string"
      """.trimMargin()

    assertThrows(CircularReferenceException::class.java) {
      ManifestParser(yaml).manifestMap
    }
  }

  @Test
  fun `ensure processManifestYaml gets streams`() {
    val yaml =
      """
            |definitions:
            |   streams:
            |     cards:
            |       name: CARDS
            |     collections:
            |       name: COLLECTIONS
            |     
            |streams:
            |  - stream1
            |  - stream2
            |  - ${"\$ref"}: "#/definitions/streams/cards"
            |  - ${"\$ref"}: "#/definitions/streams/collections"
            |manifestYamlString: "string"
      """.trimMargin()

    val expected = listOf("stream1", "stream2", mapOf("name" to "CARDS"), mapOf("name" to "COLLECTIONS"))
    val result = ManifestParser(yaml).streams
    assertEquals(expected, result)
  }

  @Test
  fun `ensure processManifestYaml handles quotes`() {
    val yaml =
      """
            |definitions:
            |   streams:
            |     cards:
            |       name: CARDS
            |     collections:
            |       name: "{{ config[\\\"api_key\\\"] }}"
            |     
            |streams:
            |  - "stream1"
            |  - "stream2"
            |  - ${"\$ref"}: "#/definitions/streams/cards"
            |  - ${"\$ref"}: "#/definitions/streams/collections"
            |manifestYamlString: "string"
      """.trimMargin()

    val expected = listOf("stream1", "stream2", mapOf("name" to "CARDS"), mapOf("name" to "{{ config[\"api_key\"] }}"))
    val result = ManifestParser(yaml).streams
    assertEquals(expected, result)
  }

  @Test
  fun `ensure processManifestYaml throws contribution error on invalid yaml`() {
    val yaml = "invalid: yaml: string"

    assertThrows(ManifestParserException::class.java) {
      ManifestParser(yaml)
    }
  }

  @Test
  fun `ensure streams throws cast error on no streams or spec found`() {
    val yaml =
      """
            |allowedHosts:
            |  - host1
            |  - host2
            |manifestYamlString: "string"
      """.trimMargin()

    assertThrows(ManifestParserException::class.java) {
      ManifestParser(yaml).streams
    }

    assertThrows(ManifestParserException::class.java) {
      ManifestParser(yaml).spec
    }
  }
}
