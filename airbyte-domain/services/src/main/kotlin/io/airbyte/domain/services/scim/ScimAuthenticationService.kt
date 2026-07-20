/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimAuthenticationException
import jakarta.inject.Singleton

@Singleton
open class ScimAuthenticationService(
  private val accessGate: ScimAccessGate,
  private val repository: ScimConfigurationRepository,
  private val tokenService: ScimTokenService,
) {
  open fun authenticate(rawToken: String): ScimAuthenticationContext {
    if (!tokenService.isValidToken(rawToken)) {
      throw ScimAuthenticationException()
    }

    val tokenHash = tokenService.hashToken(rawToken)
    val configuration = repository.findEnabledByTokenHash(tokenHash) ?: throw ScimAuthenticationException()
    val organizationId = OrganizationId(configuration.organizationId)
    if (!accessGate.isAllowed(organizationId)) {
      throw ScimAccessDeniedException("SCIM access is denied")
    }

    return ScimAuthenticationContext(
      configurationId = configuration.id ?: throw ScimAuthenticationException(),
      organizationId = organizationId,
      tokenHash = tokenHash,
    )
  }
}
