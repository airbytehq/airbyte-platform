/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.commons.json.Jsons
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.http.HttpClient
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
}
