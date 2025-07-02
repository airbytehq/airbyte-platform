/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.model.generated.ProblemSSOSetupData
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.domain.models.SsoConfig
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class SsoConfigDomainService internal constructor(
  private val ssoConfigService: SsoConfigService,
  private val organizationEmailDomainService: OrganizationEmailDomainService,
  private val airbyteKeycloakClient: AirbyteKeycloakClient,
) {
  @Transactional("config")
  open fun createAndStoreSsoConfig(config: SsoConfig) {
    try {
      airbyteKeycloakClient.createOidcSsoConfig(config)
    } catch (ex: Exception) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(ex.message),
      )
    }

    ssoConfigService.createSsoConfig(config)

    organizationEmailDomainService.createEmailDomain(
      OrganizationEmailDomain().apply {
        id = UUID.randomUUID()
        organizationId = config.organizationId
        emailDomain = config.emailDomain
      },
    )
  }
}
