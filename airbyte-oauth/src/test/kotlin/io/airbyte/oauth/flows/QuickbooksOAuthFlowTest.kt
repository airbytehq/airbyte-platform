/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

internal class QuickbooksOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() {
      val clock = Clock.fixed(Instant.ofEpochSecond(1673464409), ZoneId.of("UTC"))
      return QuickbooksOAuthFlow(httpClient, { this.constantState }, clock)
    }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://appcenter.intuit.com/app/connect/oauth2?client_id=test_client_id&scope=com.intuit.quickbooks.accounting&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state"

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.of<String, Any>(
          "token_expiry_date",
          java.util.Map.of<String, String>("type", "string"),
          "access_token",
          java.util.Map.of<String, String>("type", "string"),
          "refresh_token",
          java.util.Map.of<String, String>("type", "string"),
          "realm_id",
          java.util.Map.of<String, String>("type", "string"),
        ),
      )

  override val expectedOutput: Map<String, String>
    get() =
      java.util.Map.of(
        "expires_in",
        "720",
        "refresh_token",
        "refresh_token_response",
        "access_token",
        "access_token_response",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
        "client_secret",
        MoreOAuthParameters.SECRET_MASK,
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      java.util.Map.of(
        "token_expiry_date",
        "2023-01-11T19:25:29Z",
        "refresh_token",
        "refresh_token_response",
        "access_token",
        "access_token_response",
        "realm_id",
        "realmId",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
      )

  override val queryParams: Map<String, Any>
    get() =
      java.util.Map.of<String, Any>(
        "code",
        "test_code",
        "realmId",
        "realmId",
      )

  @Test
  override fun testDeprecatedCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteSourceOAuth() {
  }

  @Test
  override fun testCompleteDestinationOAuth() {
  }

  @Test
  override fun testEmptyInputCompleteDestinationOAuth() {
  }
}
