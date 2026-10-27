/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.services.SsoConfigService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

/** Reads only the saved realm association; provider credentials are not part of this projection. */
@Singleton
class SsoAuthorizationDataService(
  private val configurations: SsoConfigService,
) {
  fun savedRealm(organizationId: UUID): Optional<String> {
    val configuration = configurations.getSsoConfig(organizationId) ?: return Optional.empty()
    if (configuration.organizationId != organizationId) throw AuthorizationResourceNotFoundException()
    val realm: String? = configuration.keycloakRealm
    if (realm.isNullOrBlank()) throw AuthorizationResourceNotFoundException()
    return Optional.of(realm)
  }
}
