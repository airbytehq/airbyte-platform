/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.oauth.REFRESH_TOKEN_KEY
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
      getJsonSchema(
        mapOf(
          "token_expiry_date" to mapOf("type" to "string"),
          "access_token" to mapOf("type" to "string"),
          REFRESH_TOKEN_KEY to mapOf("type" to "string"),
          "realm_id" to mapOf("type" to "string"),
        ),
      )

  override val expectedOutput: Map<String, String>
    get() =
      mapOf(
        "expires_in" to "720",
        REFRESH_TOKEN_KEY to "refresh_token_response",
        "access_token" to "access_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      mapOf(
        "token_expiry_date" to "2023-01-11T19:25:29Z",
        REFRESH_TOKEN_KEY to "refresh_token_response",
        "access_token" to "access_token_response",
        "realm_id" to "realmId",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val queryParams: Map<String, Any>
    get() =
      mapOf(
        AUTH_CODE_KEY to "test_code",
        "realmId" to "realmId",
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
