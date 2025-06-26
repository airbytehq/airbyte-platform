/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative

import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.security.SecureRandom
import java.util.List

internal class DeclarativeOAuthSpecHandlerTest {
  private val secureRandom: SecureRandom = Mockito.mock(SecureRandom::class.java)
  private val handler = DeclarativeOAuthSpecHandler()

  @Test
  fun testGetStateKey() {
    val userConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          DeclarativeOAuthSpecHandler.STATE_KEY,
          DeclarativeOAuthSpecHandler.STATE_VALUE,
        ),
      )

    Assertions.assertEquals(DeclarativeOAuthSpecHandler.STATE_VALUE, handler.getStateKey(userConfig))
  }

  @Test
  fun testGetConfigurableState() {
    val stateConfig = Jsons.jsonNode(java.util.Map.of("min", 7, "max", 10))
    Mockito.`when`(secureRandom.nextInt(ArgumentMatchers.anyInt())).thenReturn(5)
    Assertions.assertNotNull(handler.getConfigurableState(stateConfig))
  }

  @Test
  fun testCreateDefaultTemplateMap() {
    val userConfig = Jsons.jsonNode(java.util.Map.of(DeclarativeOAuthSpecHandler.CLIENT_ID_KEY, TEST_CLIENT_ID))
    val templateMap: Map<String?, String?> = handler.createDefaultTemplateMap(userConfig)
    Assertions.assertEquals(
      TEST_CLIENT_ID,
      templateMap[DeclarativeOAuthSpecHandler.CLIENT_ID_KEY],
    )
  }

  @Test
  fun testGetConsentUrlTemplateValues() {
    val userConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          DeclarativeOAuthSpecHandler.CLIENT_ID_KEY,
          TEST_CLIENT_ID,
          DeclarativeOAuthSpecHandler.REDIRECT_URI_KEY,
          TEST_REDIRECT_URI,
          DeclarativeOAuthSpecHandler.STATE_KEY,
          TEST_STATE,
        ),
      )

    val templateValues = handler.getConsentUrlTemplateValues(userConfig, TEST_CLIENT_ID, TEST_REDIRECT_URI, TEST_STATE)
    Assertions.assertEquals(
      TEST_CLIENT_ID,
      templateValues[DeclarativeOAuthSpecHandler.CLIENT_ID_KEY],
    )
    Assertions.assertEquals(
      TEST_REDIRECT_URI,
      templateValues[DeclarativeOAuthSpecHandler.REDIRECT_URI_KEY],
    )
    Assertions.assertEquals(
      TEST_STATE,
      templateValues[DeclarativeOAuthSpecHandler.STATE_KEY],
    )
  }

  @Test
  @Throws(IOException::class)
  fun testRenderStringTemplate() {
    val templateValues = java.util.Map.of("key", "value")
    val templateString = "{{ key }}"
    val expected = "value"
    Assertions.assertEquals(expected, handler.renderStringTemplate(templateValues, templateString))
  }

  @Test
  fun testGetConfigExtractOutput() {
    val userConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          DeclarativeOAuthSpecHandler.EXTRACT_OUTPUT_KEY,
          List.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN),
        ),
      )

    val extractOutput = handler.getConfigExtractOutput(userConfig)
    Assertions.assertEquals(List.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN), extractOutput)
  }

  @Test
  @Throws(IOException::class)
  fun testRenderCompleteOAuthHeaders() {
    val userConfig =
      Jsons.jsonNode(
        java.util.Map.of(
          DeclarativeOAuthSpecHandler.ACCESS_TOKEN_HEADERS_KEY,
          java.util.Map.of("{{ key }}", "{{ value }}"),
        ),
      )

    val templateValues = java.util.Map.of("key", "header_key", "value", "header_value")
    val headers = handler.renderCompleteOAuthHeaders(templateValues, userConfig)
    Assertions.assertEquals("header_value", headers["header_key"])
  }

  /**
   * Tests the processOAuthOutput method of the DeclarativeOAuthSpecHandler class.
   *
   * Examples:
   *
   * Input: ["access_token", "refresh_token"] Output: {"access_token": "access_token_value",
   * "refresh_token": "refresh_token_value"}
   *
   * This test verifies that the processOAuthOutput method correctly extracts the access token from
   * the provided user configuration and data.
   *
   */
  @Test
  @Throws(IOException::class)
  fun testProcessOAuthOutput() {
    val extractOutputInputValues = List.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN, DeclarativeOAuthSpecHandler.REFRESH_TOKEN)

    val userConfig = Jsons.jsonNode(java.util.Map.of(DeclarativeOAuthSpecHandler.EXTRACT_OUTPUT_KEY, extractOutputInputValues))
    val jsonData =
      Jsons.jsonNode(
        java.util.Map.of(
          DeclarativeOAuthSpecHandler.ACCESS_TOKEN,
          ACCESS_TOKEN_TEST_VALUE,
          DeclarativeOAuthSpecHandler.REFRESH_TOKEN,
          REFRESH_TOKEN_TEST_VALUE,
        ),
      )

    val output = handler.processOAuthOutput(userConfig, jsonData, TEST_ACCESS_TOKEN_URL)

    Assertions.assertEquals(
      ACCESS_TOKEN_TEST_VALUE,
      output[DeclarativeOAuthSpecHandler.ACCESS_TOKEN],
    )
    Assertions.assertEquals(
      REFRESH_TOKEN_TEST_VALUE,
      output[DeclarativeOAuthSpecHandler.REFRESH_TOKEN],
    )
  }

  /**
   * Tests the processOAuthOutput method to ensure it correctly extracts OAuth tokens from a nested
   * JSON data structure.
   *
   * Examples:
   *
   * Input: ["main_data.nested_data.auth_data.access_token", "main_data.nested_data.refresh_token"]
   * Output: {"access_token": "access_token_value", "refresh_token": "refresh_token_value"}
   *
   * The test constructs a JSON input with nested data, specifies the paths to the access token and
   * refresh token, and verifies that the processOAuthOutput method correctly extracts these tokens
   * into a map.
   */
  @Test
  @Throws(IOException::class)
  fun testProcessOAuthOutputFromNestedDataObject() {
    val accessTokenEntry = "data.nested.auth." + DeclarativeOAuthSpecHandler.ACCESS_TOKEN
    val refreshTokenEntry = "data.nested." + DeclarativeOAuthSpecHandler.REFRESH_TOKEN
    val extractOutputInputValues = List.of(accessTokenEntry, refreshTokenEntry)

    val userConfig = Jsons.jsonNode(java.util.Map.of(DeclarativeOAuthSpecHandler.EXTRACT_OUTPUT_KEY, extractOutputInputValues))
    val jsonData =
      Jsons.jsonNode(
        java.util.Map.of(
          "data",
          java.util.Map.of(
            "nested",
            java.util.Map.of(
              DeclarativeOAuthSpecHandler.REFRESH_TOKEN,
              REFRESH_TOKEN_TEST_VALUE,
              "auth",
              java.util.Map.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN, ACCESS_TOKEN_TEST_VALUE),
            ),
          ),
        ),
      )

    val output = handler.processOAuthOutput(userConfig, jsonData, TEST_ACCESS_TOKEN_URL)

    Assertions.assertEquals(
      ACCESS_TOKEN_TEST_VALUE,
      output[DeclarativeOAuthSpecHandler.ACCESS_TOKEN],
    )
    Assertions.assertEquals(
      REFRESH_TOKEN_TEST_VALUE,
      output[DeclarativeOAuthSpecHandler.REFRESH_TOKEN],
    )
  }

  companion object {
    private const val ACCESS_TOKEN_TEST_VALUE = "access_token_value"
    private const val REFRESH_TOKEN_TEST_VALUE = "refresh_token_value"
    private const val TEST_ACCESS_TOKEN_URL = "test_access_token_url"
    private const val TEST_CLIENT_ID = "test_client_id"
    private const val TEST_REDIRECT_URI = "test_redirect_uri"
    private const val TEST_STATE = "test_state"
  }
}
