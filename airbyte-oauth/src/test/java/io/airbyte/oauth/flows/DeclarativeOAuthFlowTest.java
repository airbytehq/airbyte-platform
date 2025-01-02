/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.commons.json.Jsons;
import io.airbyte.oauth.BaseOAuthFlow;
import io.airbyte.oauth.MoreOAuthParameters;
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class DeclarativeOAuthFlowTest extends BaseOAuthFlowTest {

  private static final String ACCESS_TOKEN = "access_token";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String CLIENT_ID = "client_id";
  private static final String CLIENT_SECRET = "client_secret";
  private static final String EXPIRES_IN = "expires_in";
  private static final String TOKEN_EXPIRY_DATE = "token_expiry_date";

  // JsonSchema Types
  private static final JsonNode STRING_TYPE = JsonNodeFactory.instance.objectNode().put("type", "string");
  private static final JsonNode ARRAY_TYPE = JsonNodeFactory.instance.objectNode().put("type", "array").set("items", STRING_TYPE);
  private static final JsonNode OBJECT_TYPE = JsonNodeFactory.instance.objectNode().put("type", "object");

  @Override
  protected BaseOAuthFlow getOAuthFlow() {
    final DeclarativeOAuthFlow oauthFlow = new DeclarativeOAuthFlow(getHttpClient(), this::getConstantState);
    oauthFlow.specHandler.setClock(Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneId.of("UTC")));
    return oauthFlow;
  }

  @Override
  protected String getExpectedConsentUrl() {
    final String expectedClientId = "test_client_id";
    final String expectedRedirectUri = "https%3A%2F%2Fairbyte.io";
    final String expectedScope = "test_scope_1+test_scope_2+test_scope_3";
    final String expectedState = "state";
    final String expectedSubdomain = "test_subdomain";
    // Base64 from (String) of `state`, using `SHA-256`.
    final String expectedCodeChallenge = "S6aXNcpTdl7WpwnttWxuoja3GTo7KaazkMNG8PQ0Dk4=";

    return String.format(
        "https://some.domain.com/oauth2/authorize?my_client_id_key=%s&callback_uri=%s&scope=%s&my_state_key=%s&subdomain=%s&code_challenge=%s",
        expectedClientId,
        expectedRedirectUri,
        expectedScope,
        expectedState,
        expectedSubdomain,
        expectedCodeChallenge);
  }

  @Override
  protected JsonNode getInputOAuthConfiguration() {
    return Jsons.jsonNode(Map.ofEntries(
        // the `subdomain` is a custom property passed by the user (test)
        Map.entry("subdomain", "test_subdomain"),
        // these are the part of the spec,
        // not all spec properties are provided, since they provide an override to the default values.
        Map.entry("consent_url",
            "https://some.domain.com/oauth2/authorize?{client_id_key}={{client_id_key}}&{redirect_uri_key}={urlEncoder:{{redirect_uri_key}}}&{scope_key}={urlEncoder:{{scope_key}}}&{state_key}={{state_key}}&subdomain={subdomain}&code_challenge={codeChallengeS256:{{state_key}}}"),
        Map.entry("scope", "test_scope_1 test_scope_2 test_scope_3"),
        Map.entry("access_token_url", "https://some.domain.com/oauth2/token/"),
        Map.entry("access_token_headers", Jsons.jsonNode(Map.of("test_header", "test_value"))),
        // Map.entry("state", Jsons.jsonNode(Map.of("min", 43, "max", 128))),
        Map.entry("state_key", "my_state_key"),
        Map.entry("client_id_key", "my_client_id_key"),
        Map.entry("client_secret_key", "my_client_secret_key"),
        Map.entry("auth_code_key", "my_auth_code_key"),
        Map.entry("redirect_uri_key", "callback_uri"),
        Map.entry("extract_output", Jsons.jsonNode(List.of(ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES_IN)))));
  }

  @Override
  protected JsonNode getUserInputFromConnectorConfigSpecification() {
    return getJsonSchema(Map.ofEntries(
        // the `subdomain` is a custom property passed by the user (test)
        Map.entry("subdomain", STRING_TYPE),
        // these are the part of the spec
        Map.entry("consent_url", STRING_TYPE),
        Map.entry("scope", STRING_TYPE),
        Map.entry("state", OBJECT_TYPE),
        Map.entry("access_token_url", STRING_TYPE),
        Map.entry("extract_output", ARRAY_TYPE),
        Map.entry("access_token_headers", OBJECT_TYPE),
        Map.entry("client_id_key", STRING_TYPE),
        Map.entry("client_secret_key", STRING_TYPE),
        Map.entry("scope_key", STRING_TYPE),
        Map.entry("state_key", STRING_TYPE),
        Map.entry("auth_code_key", STRING_TYPE),
        Map.entry("redirect_uri_key", STRING_TYPE)));
  }

  @Override
  protected Map<String, String> getExpectedOutput() {
    return Map.of(
        EXPIRES_IN, "7200",
        ACCESS_TOKEN, "access_token_response",
        REFRESH_TOKEN, "refresh_token_response",
        CLIENT_ID, MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET, MoreOAuthParameters.SECRET_MASK);
  }

  @Override
  protected JsonNode getCompleteOAuthOutputSpecification() {
    return getJsonSchema(Map.of(
        TOKEN_EXPIRY_DATE, STRING_TYPE,
        ACCESS_TOKEN, STRING_TYPE,
        REFRESH_TOKEN, STRING_TYPE,
        CLIENT_ID, STRING_TYPE));
  }

  @Override
  protected Map<String, String> getExpectedFilteredOutput() {
    return Map.of(
        TOKEN_EXPIRY_DATE, "2024-01-01T02:00:00Z",
        ACCESS_TOKEN, "access_token_response",
        REFRESH_TOKEN, "refresh_token_response",
        CLIENT_ID, MoreOAuthParameters.SECRET_MASK);
  }

  @Override
  protected Map<String, Object> getQueryParams() {
    return Map.of(
        // keys override
        "my_auth_code_key", "test_code",
        "my_state_key", getConstantState(),
        // default test values
        "code", "test_code",
        "state", getConstantState());
  }

  @Test
  @Override
  void testGetSourceConsentUrlEmptyOAuthSpec() {}

  @Test
  @Override
  void testGetDestinationConsentUrlEmptyOAuthSpec() {}

  @Test
  @Override
  void testDeprecatedCompleteDestinationOAuth() {}

  @Test
  @Override
  void testDeprecatedCompleteSourceOAuth() {}

  @Test
  @Override
  void testEmptyInputCompleteSourceOAuth() {}

  @Test
  @Override
  void testEmptyInputCompleteDestinationOAuth() {}

}
