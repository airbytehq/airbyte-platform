/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import org.junit.jupiter.api.Test
import java.util.Map

internal class MicrosoftTeamsOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = MicrosoftTeamsOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&state=state&scope=offline_access+Application.Read.All+Channel.ReadBasic.All+ChannelMember.Read.All+ChannelMember.ReadWrite.All+ChannelSettings.Read.All+ChannelSettings.ReadWrite.All+Directory.Read.All+Directory.ReadWrite.All+Files.Read.All+Files.ReadWrite.All+Group.Read.All+Group.ReadWrite.All+GroupMember.Read.All+Reports.Read.All+Sites.Read.All+Sites.ReadWrite.All+TeamsTab.Read.All+TeamsTab.ReadWrite.All+User.Read.All+User.ReadWrite.All&response_type=code"

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        Map.of(
          "tenant_id",
          "test_tenant_id",
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        Map.of<String, Any>(
          "tenant_id",
          Map.of<String, String>("type", "string"),
        ),
      )

  @Test
  override fun testEmptyInputCompleteSourceOAuth() {
  }

  @Test
  override fun testEmptyInputCompleteDestinationOAuth() {
  }
}
