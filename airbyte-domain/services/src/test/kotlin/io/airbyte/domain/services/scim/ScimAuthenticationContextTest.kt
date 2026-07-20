/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.domain.models.OrganizationId
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class ScimAuthenticationContextTest {
  private val configurationId = UUID.randomUUID()
  private val organizationId = OrganizationId(UUID.randomUUID())

  @Test
  fun `matches only the authenticated token hash`() {
    val context = ScimAuthenticationContext(configurationId, organizationId, "authenticated-hash")

    assertTrue(context.matchesTokenHash("authenticated-hash"))
    assertFalse(context.matchesTokenHash("replacement-hash"))
    assertFalse(context.matchesTokenHash(null))
  }

  @Test
  fun `never renders its token hash`() {
    val context = ScimAuthenticationContext(configurationId, organizationId, "secret-token-hash")

    val rendered = context.toString()
    assertFalse(rendered.contains("secret-token-hash"))
    assertTrue(rendered.contains(configurationId.toString()))
    assertTrue(rendered.contains(organizationId.toString()))
    assertTrue(rendered.contains("REDACTED"))
  }
}
