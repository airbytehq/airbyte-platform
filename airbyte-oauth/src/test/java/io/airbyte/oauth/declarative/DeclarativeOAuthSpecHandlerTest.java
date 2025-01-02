/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeclarativeOAuthSpecHandlerTest {

  private final SecureRandom secureRandom = mock(SecureRandom.class);
  private final DeclarativeOAuthSpecHandler handler = new DeclarativeOAuthSpecHandler();

  private static final String ACCESS_TOKEN_TEST_VALUE = "access_token_value";
  private static final String REFRESH_TOKEN_TEST_VALUE = "refresh_token_value";
  private static final String TEST_ACCESS_TOKEN_URL = "test_access_token_url";
  private static final String TEST_CLIENT_ID = "test_client_id";
  private static final String TEST_REDIRECT_URI = "test_redirect_uri";
  private static final String TEST_STATE = "test_state";

  @Test
  void testGetStateKey() {
    final JsonNode userConfig = Jsons.jsonNode(
        Map.of(
            DeclarativeOAuthSpecHandler.STATE_KEY, DeclarativeOAuthSpecHandler.STATE_VALUE));

    assertEquals(DeclarativeOAuthSpecHandler.STATE_VALUE, handler.getStateKey(userConfig));
  }

  @Test
  void testGetConfigurableState() {
    final JsonNode stateConfig = Jsons.jsonNode(Map.of("min", 7, "max", 10));
    when(secureRandom.nextInt(anyInt())).thenReturn(5);
    assertNotNull(handler.getConfigurableState(stateConfig));
  }

  @Test
  void testCreateDefaultTemplateMap() {
    final JsonNode userConfig = Jsons.jsonNode(Map.of(DeclarativeOAuthSpecHandler.CLIENT_ID_KEY, TEST_CLIENT_ID));
    final Map<String, String> templateMap = handler.createDefaultTemplateMap(userConfig);
    assertEquals(TEST_CLIENT_ID, templateMap.get(DeclarativeOAuthSpecHandler.CLIENT_ID_KEY));
  }

  @Test
  void testGetConsentUrlTemplateValues() {
    final JsonNode userConfig = Jsons.jsonNode(
        Map.of(
            DeclarativeOAuthSpecHandler.CLIENT_ID_KEY, TEST_CLIENT_ID,
            DeclarativeOAuthSpecHandler.REDIRECT_URI_KEY, TEST_REDIRECT_URI,
            DeclarativeOAuthSpecHandler.STATE_KEY, TEST_STATE));

    final Map<String, String> templateValues = handler.getConsentUrlTemplateValues(userConfig, TEST_CLIENT_ID, TEST_REDIRECT_URI, TEST_STATE);
    assertEquals(TEST_CLIENT_ID, templateValues.get(DeclarativeOAuthSpecHandler.CLIENT_ID_KEY));
    assertEquals(TEST_REDIRECT_URI, templateValues.get(DeclarativeOAuthSpecHandler.REDIRECT_URI_KEY));
    assertEquals(TEST_STATE, templateValues.get(DeclarativeOAuthSpecHandler.STATE_KEY));
  }

  @Test
  void testRenderStringTemplate() throws IOException {
    final Map<String, String> templateValues = Map.of("key", "value");
    final String templateString = "This is a {key}";
    final String expected = "This is a value";
    assertEquals(expected, handler.renderStringTemplate(templateValues, templateString));
  }

  @Test
  void testCheckContext() {
    final String templateString = "This string contains a valid variable {test}";
    assertDoesNotThrow(() -> handler.checkContext(templateString));
  }

  @Test
  void testCheckContextRestricted() {
    final String restrictedString = "This string contains a restricted variable {env:test_value}";
    IOException exception = assertThrows(IOException.class, () -> handler.checkContext(restrictedString));
    final String expected = "DeclarativeOAuthSpecHandler(): the `env:` usage in "
        + "`This string contains a restricted variable {env:test_value}` is not allowed for string interpolation.";
    assertEquals(expected, exception.getMessage());
  }

  @Test
  void testGetConfigExtractOutput() {
    final JsonNode userConfig = Jsons.jsonNode(
        Map.of(
            DeclarativeOAuthSpecHandler.EXTRACT_OUTPUT_KEY, List.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN)));

    final List<String> extractOutput = handler.getConfigExtractOutput(userConfig);
    assertEquals(List.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN), extractOutput);
  }

  @Test
  void testRenderCompleteOAuthHeaders() throws IOException {
    final JsonNode userConfig = Jsons.jsonNode(
        Map.of(
            DeclarativeOAuthSpecHandler.ACCESS_TOKEN_HEADERS_KEY, Map.of("header_key", "header_value")));

    final Map<String, String> templateValues = Map.of("key", "value");
    final Map<String, String> headers = handler.renderCompleteOAuthHeaders(templateValues, userConfig);
    assertEquals("header_value", headers.get("header_key"));
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
  void testProcessOAuthOutput() throws IOException {
    final List<String> extractOutputInputValues = List.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN, DeclarativeOAuthSpecHandler.REFRESH_TOKEN);

    final JsonNode userConfig = Jsons.jsonNode(Map.of(DeclarativeOAuthSpecHandler.EXTRACT_OUTPUT_KEY, extractOutputInputValues));
    final JsonNode jsonData = Jsons.jsonNode(
        Map.of(
            DeclarativeOAuthSpecHandler.ACCESS_TOKEN, ACCESS_TOKEN_TEST_VALUE,
            DeclarativeOAuthSpecHandler.REFRESH_TOKEN, REFRESH_TOKEN_TEST_VALUE));

    final Map<String, Object> output = handler.processOAuthOutput(userConfig, jsonData, TEST_ACCESS_TOKEN_URL);

    assertEquals(ACCESS_TOKEN_TEST_VALUE, output.get(DeclarativeOAuthSpecHandler.ACCESS_TOKEN));
    assertEquals(REFRESH_TOKEN_TEST_VALUE, output.get(DeclarativeOAuthSpecHandler.REFRESH_TOKEN));
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
  void testProcessOAuthOutputFromNestedDataObject() throws IOException {
    final String accessTokenEntry = "data.nested.auth." + DeclarativeOAuthSpecHandler.ACCESS_TOKEN;
    final String refreshTokenEntry = "data.nested." + DeclarativeOAuthSpecHandler.REFRESH_TOKEN;
    final List<String> extractOutputInputValues = List.of(accessTokenEntry, refreshTokenEntry);

    final JsonNode userConfig = Jsons.jsonNode(Map.of(DeclarativeOAuthSpecHandler.EXTRACT_OUTPUT_KEY, extractOutputInputValues));
    final JsonNode jsonData = Jsons.jsonNode(
        Map.of(
            "data", Map.of(
                "nested", Map.of(
                    DeclarativeOAuthSpecHandler.REFRESH_TOKEN, REFRESH_TOKEN_TEST_VALUE,
                    "auth", Map.of(DeclarativeOAuthSpecHandler.ACCESS_TOKEN, ACCESS_TOKEN_TEST_VALUE)))));

    final Map<String, Object> output = handler.processOAuthOutput(userConfig, jsonData, TEST_ACCESS_TOKEN_URL);

    assertEquals(ACCESS_TOKEN_TEST_VALUE, output.get(DeclarativeOAuthSpecHandler.ACCESS_TOKEN));
    assertEquals(REFRESH_TOKEN_TEST_VALUE, output.get(DeclarativeOAuthSpecHandler.REFRESH_TOKEN));
  }

}
