/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.commons.json.Jsons
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.http.HttpClient
import java.util.UUID

class AmazonSellerPartnerOAuthFlowTest {
  private val httpClient: HttpClient = mockk()
  private val oauthFlow = AmazonSellerPartnerOAuthFlow(httpClient) { "state" }
  private val sourceOAuthParamConfig = Jsons.jsonNode(mapOf("app_id" to "test_app_id"))

  private fun sourceConsentUrl(
    accountType: String,
    region: String,
  ): String =
    oauthFlow.getSourceConsentUrl(
      UUID.randomUUID(),
      UUID.randomUUID(),
      REDIRECT_URL,
      Jsons.jsonNode(mapOf("account_type" to accountType, "region" to region)),
      null,
      sourceOAuthParamConfig,
    )

  @Test
  fun testSellerConsentUrlForIreland() {
    val consentUrl = sourceConsentUrl("Seller", "IE")
    assertFalse(consentUrl.startsWith("null"), "Consent URL for IE must not be built from a null region domain")
    assertTrue(consentUrl.startsWith("https://sellercentral-europe.amazon.com/apps/authorize/consent"), consentUrl)
    assertTrue(consentUrl.contains("application_id=test_app_id"), consentUrl)
  }

  @Test
  fun testVendorConsentUrlForIreland() {
    val consentUrl = sourceConsentUrl("Vendor", "IE")
    assertFalse(consentUrl.startsWith("null"), "Consent URL for IE must not be built from a null region domain")
    assertTrue(consentUrl.startsWith("https://vendorcentral.amazon.ie/apps/authorize/consent"), consentUrl)
  }

  @Test
  fun testSellerConsentUrlForExistingRegionRemainsValid() {
    val consentUrl = sourceConsentUrl("Seller", "DE")
    assertTrue(consentUrl.startsWith("https://sellercentral-europe.amazon.com/apps/authorize/consent"), consentUrl)
  }

  companion object {
    private const val REDIRECT_URL = "https://airbyte.io"
  }
}
