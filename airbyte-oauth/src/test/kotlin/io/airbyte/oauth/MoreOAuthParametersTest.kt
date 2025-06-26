/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.MoreOAuthParameters.flattenOAuthConfig
import io.airbyte.oauth.MoreOAuthParameters.mergeJsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.UUID

internal class MoreOAuthParametersTest {
  @Test
  fun testFlattenConfig() {
    val nestedConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          FIELD,
          "value1",
          "top-level",
          java.util.Map.of(
            "nested_field",
            "value2",
          ),
        ),
      )
    val expectedConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          FIELD,
          "value1",
          "nested_field",
          "value2",
        ),
      )
    val actualConfig = flattenOAuthConfig(nestedConfig)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  fun testFailureFlattenConfig() {
    val nestedConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          FIELD,
          "value1",
          "top-level",
          java.util.Map.of(
            "nested_field",
            "value2",
            FIELD,
            "value3",
          ),
        ),
      )
    Assertions.assertThrows(
      IllegalStateException::class.java,
    ) { flattenOAuthConfig(nestedConfig) }
  }

  @Test
  fun testInjectUnnestedNode() {
    val oauthParams = Jsons.jsonNode(generateOAuthParameters()) as ObjectNode

    val actual = generateJsonConfig()
    val expected = Jsons.clone(actual)
    expected.setAll<JsonNode>(oauthParams)

    mergeJsons(actual, oauthParams)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  @DisplayName("A nested config should be inserted with the same nesting structure")
  fun testInjectNewNestedNode() {
    val oauthParams = Jsons.jsonNode(generateOAuthParameters()) as ObjectNode
    val nestedConfig =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put(OAUTH_CREDS, oauthParams)
          .build(),
      ) as ObjectNode

    // nested node does not exist in actual object
    val actual = generateJsonConfig()
    val expected = Jsons.clone(actual)
    expected.putObject(OAUTH_CREDS).setAll<JsonNode>(oauthParams)

    mergeJsons(actual, nestedConfig)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  @DisplayName(
    "A nested node which partially exists in the main config should be merged into the main config, not overwrite the whole nested object",
  )
  fun testInjectedPartiallyExistingNestedNode() {
    val oauthParams = Jsons.jsonNode(generateOAuthParameters()) as ObjectNode
    val nestedConfig =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put(OAUTH_CREDS, oauthParams)
          .build(),
      ) as ObjectNode

    // nested node partially exists in actual object
    val actual = generateJsonConfig()
    actual.putObject(OAUTH_CREDS).put("irrelevant_field", "_")
    val expected = Jsons.clone(actual)
    (expected[OAUTH_CREDS] as ObjectNode).setAll<JsonNode>(oauthParams)

    mergeJsons(actual, nestedConfig)

    Assertions.assertEquals(expected, actual)
  }

  private fun generateJsonConfig(): ObjectNode =
    Jsons.jsonNode(
      ImmutableMap
        .builder<Any, Any>()
        .put("apiSecret", "123")
        .put("client", "testing")
        .build(),
    ) as ObjectNode

  private fun generateOAuthParameters(): Map<String, String> =
    ImmutableMap
      .builder<String, String>()
      .put("api_secret", "mysecret")
      .put("api_client", UUID.randomUUID().toString())
      .build()

  companion object {
    private const val FIELD = "field"
    private const val OAUTH_CREDS = "oauth_credentials"
  }
}
