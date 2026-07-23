/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.SSOConfigStatus
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.api.server.generated.models.ValidateSSOTokenRequestBody
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class SsoAuditProviderTest {
  private val provider = SsoAuditProvider()

  @Test
  fun `createSsoConfig request summary omits the clientSecret`() {
    val orgId = UUID.randomUUID()
    val request =
      CreateSSOConfigRequestBody(
        organizationId = orgId,
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "super-secret-value",
        discoveryUrl = "https://idp.example.com/.well-known/openid-configuration",
        emailDomain = "airbyte.com",
        status = SSOConfigStatus.ACTIVE,
      )

    val summary = provider.generateSummaryFromRequest(request)

    // The secret value must not appear anywhere in the serialized audit entry.
    assertFalse(summary.contains("super-secret-value"), "clientSecret value leaked into audit summary: $summary")
    assertFalse(summary.contains("clientSecret"), "clientSecret key leaked into audit summary: $summary")

    // Non-secret fields are still logged for auditability.
    val node = Jsons.deserialize(summary)
    assertEquals(orgId.toString(), node.get("organizationId").asText())
    assertEquals("airbyte", node.get("companyIdentifier").asText())
    assertEquals("client-id", node.get("clientId").asText())
    assertEquals("https://idp.example.com/.well-known/openid-configuration", node.get("discoveryUrl").asText())
    assertTrue(node.has("status"))
  }

  @Test
  fun `updateSsoCredentials request summary omits the clientSecret`() {
    val orgId = UUID.randomUUID()
    val request =
      UpdateSSOCredentialsRequestBody(
        organizationId = orgId,
        clientId = "client-id",
        clientSecret = "another-secret-value",
      )

    val summary = provider.generateSummaryFromRequest(request)

    assertFalse(summary.contains("another-secret-value"), "clientSecret value leaked into audit summary: $summary")
    assertFalse(summary.contains("clientSecret"), "clientSecret key leaked into audit summary: $summary")

    val node = Jsons.deserialize(summary)
    assertEquals(orgId.toString(), node.get("organizationId").asText())
    assertEquals("client-id", node.get("clientId").asText())
  }

  @Test
  fun `validateSsoToken request summary omits the accessToken`() {
    val request =
      ValidateSSOTokenRequestBody(
        organizationId = UUID.randomUUID(),
        accessToken = "secret-access-token",
      )

    val summary = provider.generateSummaryFromRequest(request)

    // accessToken is not allowlisted, so the allowlist strips it by default.
    assertFalse(summary.contains("secret-access-token"), "accessToken value leaked into audit summary: $summary")
    assertFalse(summary.contains("accessToken"), "accessToken key leaked into audit summary: $summary")
  }

  @Test
  fun `null request yields an empty summary`() {
    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromRequest(null))
  }
}
