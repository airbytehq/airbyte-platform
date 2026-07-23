/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.domain.models.OrganizationId
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

class ScimAuthenticationContext(
  val configurationId: UUID,
  val organizationId: OrganizationId,
  tokenHash: String,
) {
  private val authenticatedTokenHash = tokenHash.toByteArray(StandardCharsets.US_ASCII)

  fun matchesTokenHash(candidate: String?): Boolean =
    candidate != null &&
      MessageDigest.isEqual(
        authenticatedTokenHash,
        candidate.toByteArray(StandardCharsets.US_ASCII),
      )

  override fun toString(): String =
    "ScimAuthenticationContext(configurationId=$configurationId, organizationId=$organizationId, tokenHash=[REDACTED])"
}
