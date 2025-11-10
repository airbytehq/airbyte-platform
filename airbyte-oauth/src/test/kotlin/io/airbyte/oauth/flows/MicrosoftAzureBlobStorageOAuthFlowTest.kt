/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import org.junit.jupiter.api.Test

internal class MicrosoftAzureBlobStorageOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = MicrosoftAzureBlobStorageOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/authorize?client_id=test_client_id&response_type=code&redirect_uri=https%3A%2F%2Fairbyte.io&response_mode=query&state=state&scope=offline_access%20https://storage.azure.com/.default"

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
}
