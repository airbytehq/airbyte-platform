/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.util.UUID

class ScimAuditProviderTest {
  private val provider = ScimAuditProvider()

  @Test
  fun `request summary contains only explicitly allowlisted fields`() {
    val organizationId = UUID.randomUUID()
    val summary =
      provider.generateSummaryFromRequest(
        mapOf(
          "organizationId" to organizationId,
          "idpProvider" to "okta",
          "token" to "airbyte_scim_secret",
          "tokenHash" to "secret-hash",
          "futureField" to "future-value",
        ),
      )

    val node = Jsons.deserialize(summary)
    assertEquals(organizationId.toString(), node.get("organizationId").asText())
    assertEquals("okta", node.get("idpProvider").asText())
    assertEquals(setOf("organizationId", "idpProvider"), node.fieldNames().asSequence().toSet())
    assertFalse(summary.contains("airbyte_scim_secret"))
    assertFalse(summary.contains("secret-hash"))
    assertFalse(summary.contains("future-value"))
  }

  @Test
  fun `null request yields an empty summary`() {
    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromRequest(null))
  }

  @Test
  fun `result summary is always empty even when result contains a raw token`() {
    val result = mapOf("token" to "airbyte_scim_secret")

    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromResult(result))
  }
}
