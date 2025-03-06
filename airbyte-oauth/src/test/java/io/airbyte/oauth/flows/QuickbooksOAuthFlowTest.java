/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.oauth.BaseOAuthFlow;
import io.airbyte.oauth.MoreOAuthParameters;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class QuickbooksOAuthFlowTest extends BaseOAuthFlowTest {

  @Override
  protected BaseOAuthFlow getOAuthFlow() {
    final Clock clock = Clock.fixed(Instant.ofEpochSecond(1673464409), ZoneId.of("UTC"));
    return new QuickbooksOAuthFlow(getHttpClient(), this::getConstantState, clock);
  }

  @Override
  protected String getExpectedConsentUrl() {
    return "https://appcenter.intuit.com/app/connect/oauth2?client_id=test_client_id&scope=com.intuit.quickbooks.accounting&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state";
  }

  @Override
  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  protected JsonNode getCompleteOAuthOutputSpecification() {
    return getJsonSchema(Map.of(
        "token_expiry_date", Map.of("type", "string"),
        "access_token", Map.of("type", "string"),
        "refresh_token", Map.of("type", "string"),
        "realm_id", Map.of("type", "string")));
  }

  @Override
  protected Map<String, String> getExpectedOutput() {
    return Map.of(
        "expires_in", "720",
        "refresh_token", "refresh_token_response",
        "access_token", "access_token_response",
        "client_id", MoreOAuthParameters.SECRET_MASK,
        "client_secret", MoreOAuthParameters.SECRET_MASK);
  }

  @Override
  protected Map<String, String> getExpectedFilteredOutput() {
    return Map.of(
        "token_expiry_date", "2023-01-11T19:25:29Z",
        "refresh_token", "refresh_token_response",
        "access_token", "access_token_response",
        "realm_id", "realmId",
        "client_id", MoreOAuthParameters.SECRET_MASK);
  }

  @Override
  protected Map<String, Object> getQueryParams() {
    return Map.of(
        "code", "test_code",
        "realmId", "realmId");
  }

  @Test
  @Override
  void testDeprecatedCompleteDestinationOAuth() {}

  @Test
  @Override
  void testDeprecatedCompleteSourceOAuth() {}

  @Test
  @Override
  void testCompleteDestinationOAuth() {}

  @Test
  @Override
  void testEmptyInputCompleteDestinationOAuth() {}

}
