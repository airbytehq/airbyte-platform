/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.net.URLDecoder
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.Flow

internal class MicrosoftBingAdsOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = MicrosoftBingAdsOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/authorize?client_id=test_client_id&response_type=code&redirect_uri=https%3A%2F%2Fairbyte.io&response_mode=query&state=state&scope=offline_access%20https://ads.microsoft.com/msads.manage"

  override val inputOAuthConfiguration: JsonNode
    get() = Jsons.jsonNode(mapOf("tenant_id" to "test_tenant_id"))

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() = getJsonSchema(mapOf<String, Any>("tenant_id" to mapOf("type" to "string")))

  @Test
  override fun testEmptyInputCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteSourceOAuth() {
  }

  @Test
  override fun testEmptyInputCompleteSourceOAuth() {
  }

  @Test
  fun testCompleteSourceOAuthOmitsBlankClientSecret() {
    val tokenRequestParams = completeSourceOAuthTokenRequestParams("")
    assertFalse(
      tokenRequestParams.containsKey(CLIENT_SECRET_KEY),
      "A blank client secret must not be sent, otherwise Entra rejects a public-client app with AADSTS700025.",
    )
    assertEquals("authorization_code", tokenRequestParams[GRANT_TYPE_KEY])
    assertEquals("test_client_id", tokenRequestParams[CLIENT_ID_KEY])
  }

  @Test
  fun testCompleteSourceOAuthSendsConfiguredClientSecret() {
    val tokenRequestParams = completeSourceOAuthTokenRequestParams("test_client_secret")
    assertEquals("test_client_secret", tokenRequestParams[CLIENT_SECRET_KEY])
    assertEquals("authorization_code", tokenRequestParams[GRANT_TYPE_KEY])
    assertEquals("test_client_id", tokenRequestParams[CLIENT_ID_KEY])
  }

  /**
   * Runs the code exchange against the given configured client secret and returns the form-encoded
   * parameters that were actually posted to the token endpoint.
   */
  private fun completeSourceOAuthTokenRequestParams(clientSecret: String): Map<String, String> {
    val response = mockk<HttpResponse<String>>()
    every { response.body() } returns mockedResponse
    val sentRequest = slot<HttpRequest>()
    every { httpClient.send(capture(sentRequest), any<HttpResponse.BodyHandler<String>>()) } returns response

    oAuthFlow.completeSourceOAuth(
      UUID.randomUUID(),
      UUID.randomUUID(),
      queryParams,
      REDIRECT_URL,
      inputOAuthConfiguration,
      getOauthConfigSpecification(),
      Jsons.jsonNode(
        mapOf(
          CLIENT_ID_KEY to "test_client_id",
          CLIENT_SECRET_KEY to clientSecret,
        ),
      ),
    )

    return parseFormEncodedBody(readRequestBody(sentRequest.captured))
  }

  private fun parseFormEncodedBody(body: String): Map<String, String> =
    body
      .split("&")
      .filter { it.isNotEmpty() }
      .associate {
        it.substringBefore("=") to URLDecoder.decode(it.substringAfter("="), StandardCharsets.UTF_8)
      }

  private fun readRequestBody(request: HttpRequest): String {
    val bodySubscriber = HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8)
    request.bodyPublisher().orElseThrow().subscribe(
      object : Flow.Subscriber<ByteBuffer> {
        override fun onSubscribe(subscription: Flow.Subscription) = bodySubscriber.onSubscribe(subscription)

        override fun onNext(item: ByteBuffer) = bodySubscriber.onNext(listOf(item))

        override fun onError(throwable: Throwable) = bodySubscriber.onError(throwable)

        override fun onComplete() = bodySubscriber.onComplete()
      },
    )
    return bodySubscriber.body.toCompletableFuture().join()
  }

  companion object {
    private const val REDIRECT_URL = "https://airbyte.io"
  }
}
