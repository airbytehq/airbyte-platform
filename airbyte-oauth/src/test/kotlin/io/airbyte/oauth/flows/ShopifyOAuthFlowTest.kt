/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.jvm.isAccessible

/**
 * Tests for ShopifyOAuthFlow, specifically the formatConsentUrl behavior
 * that routes internal origins to the App Store and external origins to
 * the preflight endpoint.
 */
class ShopifyOAuthFlowTest {
  private lateinit var httpClient: HttpClient
  private lateinit var shopifyOAuthFlow: ShopifyOAuthFlow

  @BeforeEach
  fun setup() {
    httpClient = mockk()
    shopifyOAuthFlow = ShopifyOAuthFlow(httpClient)
  }

  /**
   * Helper to call the protected formatConsentUrl method using reflection.
   */
  private fun callFormatConsentUrl(redirectUrl: String): String {
    val method =
      ShopifyOAuthFlow::class
        .declaredMemberFunctions
        .find { it.name == "formatConsentUrl" }!!
    method.isAccessible = true
    return method.call(
      shopifyOAuthFlow,
      UUID.randomUUID(),
      "client_id",
      redirectUrl,
      Jsons.emptyObject(),
    ) as String
  }

  // ==================== Internal Origins → App Store URL ====================

  @Test
  fun testFormatConsentUrl_internalOrigin_cloudAirbyte() {
    val url = callFormatConsentUrl("https://cloud.airbyte.com/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  @Test
  fun testFormatConsentUrl_internalOrigin_subdomainAirbyteCom() {
    val url = callFormatConsentUrl("https://staging.airbyte.com/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  @Test
  fun testFormatConsentUrl_internalOrigin_localhost() {
    val url = callFormatConsentUrl("https://localhost/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  @Test
  fun testFormatConsentUrl_internalOrigin_localhostWithPort() {
    val url = callFormatConsentUrl("https://localhost:3000/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  // ==================== External Origins → Preflight URL ====================

  @Test
  fun testFormatConsentUrl_externalOrigin_sonar() {
    val url = callFormatConsentUrl("https://app.airbyte.ai/auth_flow")
    Assertions.assertTrue(url.startsWith("https://cloud.airbyte.com/partner/v1/shopify/oauth/preflight"))
    Assertions.assertTrue(url.contains("origin=https%3A%2F%2Fapp.airbyte.ai"))
  }

  // ==================== .airbyte.dev Origins → App Store URL (treated as internal) ====================

  @Test
  fun testFormatConsentUrl_internalOrigin_airbyteDev() {
    val url = callFormatConsentUrl("https://frontend-dev-cloud.internal.airbyte.dev/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  @Test
  fun testFormatConsentUrl_internalOrigin_deployPreview() {
    val url = callFormatConsentUrl("https://deploy-preview-18437-cloud.frontend-dev-preview.internal.airbyte.dev/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  @Test
  fun testFormatConsentUrl_internalOrigin_localAirbyteDev() {
    val url = callFormatConsentUrl("https://local.airbyte.dev/auth_flow")
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  @Test
  fun testFormatConsentUrl_internalOrigin_otherAirbyteDev() {
    val url = callFormatConsentUrl("https://some-environment.airbyte.dev/auth_flow")
    // .airbyte.dev is treated as internal (like .airbyte.com)
    Assertions.assertEquals("https://apps.shopify.com/airbyte", url)
  }

  // ==================== Public API Callback → Preflight URL with apiCallback ====================

  @Test
  fun testFormatConsentUrl_publicApiCallback_returnsPreflightWithApiCallback() {
    val apiCallbackUrl = "https://api.airbyte.com/v1/oauth/callback"
    val url = callFormatConsentUrl(apiCallbackUrl)

    // Should return the preflight URL, NOT the App Store URL
    Assertions.assertTrue(
      url.startsWith("https://cloud.airbyte.com/partner/v1/shopify/oauth/preflight"),
      "Expected preflight URL but got: $url",
    )
    // Should contain apiCallback parameter pointing to the public API callback
    Assertions.assertTrue(
      url.contains("apiCallback="),
      "Expected apiCallback parameter in URL: $url",
    )
    // Should contain the encoded API callback URL
    Assertions.assertTrue(
      url.contains("api.airbyte.com"),
      "Expected api.airbyte.com in apiCallback: $url",
    )
    // Should NOT be the App Store URL
    Assertions.assertFalse(
      url.contains("apps.shopify.com"),
      "Should NOT return App Store URL for public API callback: $url",
    )
  }

  @Test
  fun testFormatConsentUrl_publicApiCallback_stagingApi() {
    val url = callFormatConsentUrl("https://api.staging.airbyte.com/v1/oauth/callback")

    // Should also trigger public API flow (contains /v1/oauth/callback)
    Assertions.assertTrue(
      url.startsWith("https://cloud.airbyte.com/partner/v1/shopify/oauth/preflight"),
      "Expected preflight URL for staging API but got: $url",
    )
    Assertions.assertTrue(url.contains("apiCallback="))
  }

  @Test
  fun testFormatConsentUrl_publicApiCallback_originIsCloudAirbyte() {
    val url = callFormatConsentUrl("https://api.airbyte.com/v1/oauth/callback")

    // origin should be cloud.airbyte.com (for Shopify host matching)
    Assertions.assertTrue(
      url.contains("origin=https"),
      "Expected origin parameter in URL: $url",
    )
  }

  // ==================== Token Exchange Error Handling ====================

  @Suppress("UNCHECKED_CAST")
  private fun callCompleteOAuthFlow(
    clientId: String = "test-client-id",
    clientSecret: String = "test-client-secret",
    authCode: String = "test-auth-code",
    shopName: String = "test-store.myshopify.com",
    redirectUrl: String = "https://cloud.airbyte.com/auth_flow",
  ): Map<String, Any> {
    val method =
      ShopifyOAuthFlow::class
        .declaredMemberFunctions
        .find { it.name == "completeOAuthFlow" }!!
    method.isAccessible = true
    return method.call(
      shopifyOAuthFlow,
      clientId,
      clientSecret,
      authCode,
      shopName,
      redirectUrl,
      Jsons.emptyObject(),
      Jsons.emptyObject(),
    ) as Map<String, Any>
  }

  private fun callExtractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String = "https://test-store.myshopify.com/admin/oauth/access_token",
    shopName: String = "test-store.myshopify.com",
  ): Map<String, Any> {
    val method =
      ShopifyOAuthFlow::class
        .declaredMemberFunctions
        .find { it.name == "extractOAuthOutput" }!!
    method.isAccessible = true
    @Suppress("UNCHECKED_CAST")
    return method.call(shopifyOAuthFlow, data, accessTokenUrl, shopName) as Map<String, Any>
  }

  private fun mockHttpResponse(
    statusCode: Int,
    body: String,
  ) {
    val response = mockk<HttpResponse<String>>()
    every { response.statusCode() } returns statusCode
    every { response.body() } returns body
    every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns response
  }

  private fun assertReflectionThrowsIOException(block: () -> Unit): IOException {
    val ite =
      Assertions.assertThrows(InvocationTargetException::class.java) {
        block()
      }
    Assertions.assertInstanceOf(IOException::class.java, ite.cause)
    return ite.cause as IOException
  }

  @Test
  fun testCompleteOAuthFlow_httpError_throwsWithStatusAndError() {
    mockHttpResponse(401, """{"error": "invalid_client"}""")

    val exception = assertReflectionThrowsIOException { callCompleteOAuthFlow() }
    Assertions.assertTrue(
      exception.message!!.contains("HTTP 401"),
      "Expected HTTP status in error: ${exception.message}",
    )
    Assertions.assertTrue(
      exception.message!!.contains("invalid_client"),
      "Expected Shopify error in message: ${exception.message}",
    )
  }

  @Test
  fun testCompleteOAuthFlow_httpError400_throwsWithResponseBody() {
    mockHttpResponse(400, """{"error": "invalid_request", "error_description": "code was already used"}""")

    val exception = assertReflectionThrowsIOException { callCompleteOAuthFlow() }
    Assertions.assertTrue(exception.message!!.contains("HTTP 400"))
    Assertions.assertTrue(exception.message!!.contains("invalid_request"))
  }

  @Test
  fun testCompleteOAuthFlow_httpErrorNonJson_throwsWithRawBody() {
    mockHttpResponse(500, "Internal Server Error")

    val exception = assertReflectionThrowsIOException { callCompleteOAuthFlow() }
    Assertions.assertTrue(exception.message!!.contains("HTTP 500"))
    Assertions.assertTrue(exception.message!!.contains("Internal Server Error"))
  }

  @Test
  fun testCompleteOAuthFlow_success_returnsAccessTokenAndShop() {
    mockHttpResponse(200, """{"access_token": "shpat_test123", "scope": "read_products"}""")

    val result = callCompleteOAuthFlow(shopName = "my-store.myshopify.com")
    Assertions.assertEquals("shpat_test123", result["access_token"])
    Assertions.assertEquals("my-store.myshopify.com", result["shop"])
  }

  @Test
  fun testExtractOAuthOutput_missingAccessToken_withErrorField() {
    val data = Jsons.deserialize("""{"error": "invalid_client"}""")

    val exception = assertReflectionThrowsIOException { callExtractOAuthOutput(data) }
    Assertions.assertTrue(
      exception.message!!.contains("Missing 'access_token'"),
      "Expected missing access_token message: ${exception.message}",
    )
    Assertions.assertTrue(
      exception.message!!.contains("invalid_client"),
      "Expected error field in message: ${exception.message}",
    )
    Assertions.assertFalse(
      exception.message!!.contains("query params"),
      "Should not say 'query params': ${exception.message}",
    )
  }

  @Test
  fun testExtractOAuthOutput_missingAccessToken_noErrorField() {
    val data = Jsons.deserialize("""{"scope": "read_products"}""")

    val exception = assertReflectionThrowsIOException { callExtractOAuthOutput(data) }
    Assertions.assertTrue(exception.message!!.contains("Missing 'access_token'"))
    Assertions.assertTrue(
      exception.message!!.contains("response="),
      "Expected raw response in message: ${exception.message}",
    )
  }

  @Test
  fun testExtractOAuthOutput_success() {
    val data = Jsons.deserialize("""{"access_token": "shpat_abc123"}""")

    val result = callExtractOAuthOutput(data, shopName = "my-store.myshopify.com")
    Assertions.assertEquals("shpat_abc123", result["access_token"])
    Assertions.assertEquals("my-store.myshopify.com", result["shop"])
  }
}
