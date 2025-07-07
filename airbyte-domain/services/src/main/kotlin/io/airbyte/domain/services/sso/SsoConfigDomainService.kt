/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.model.generated.ProblemSSOSetupData
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
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
  private val organizationService: OrganizationService,
) {
  @Transactional("config")
  open fun createAndStoreSsoConfig(config: SsoConfig) {
    validateEmailDomain(config)
    validateExistingEmailDomainEntries(config)
    validateExistingSsoConfig(config)

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

  /**
   * This function checks if the email that is associated with the organization
   * in the SsoConfig matches the domain passed in to the sso config creation request.
   * At this time, we don't have officially validated emails, so we attempt to prevent
   * users from creating random sso email domains by trying to match their desired
   * domain with the domain from their org.
   */
  @InternalForTesting
  internal fun validateEmailDomain(config: SsoConfig) {
    val org = organizationService.getOrganization(config.organizationId)
    if (org.isEmpty) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Organization with id ${config.organizationId} not found"),
      )
    }

    val orgEmailDomain =
      org
        .get()
        .email
        .split("@")
        .getOrNull(1)

    if (orgEmailDomain == null || orgEmailDomain != config.emailDomain) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Invalid email domain: $orgEmailDomain. Domain must match the organization"),
      )
    }
  }

  private fun validateExistingEmailDomainEntries(config: SsoConfig) {
    val existingEmailDomains = organizationEmailDomainService.findByEmailDomain(config.emailDomain)
    if (existingEmailDomains.isNotEmpty()) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Email domain already exists: ${config.emailDomain}"),
      )
    }
  }

  private fun validateExistingSsoConfig(config: SsoConfig) {
    val existingConfig = ssoConfigService.getSsoConfig(config.organizationId)
    if (existingConfig != null) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("SSO Config already exists for organization ${config.organizationId}"),
      )
    }
  }
}
