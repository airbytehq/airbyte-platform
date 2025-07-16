/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.model.generated.ProblemSSOConfigRetrievalData
import io.airbyte.api.problems.model.generated.ProblemSSOCredentialUpdateData
import io.airbyte.api.problems.model.generated.ProblemSSODeletionData
import io.airbyte.api.problems.model.generated.ProblemSSOSetupData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.SSOConfigRetrievalProblem
import io.airbyte.api.problems.throwable.generated.SSOCredentialUpdateProblem
import io.airbyte.api.problems.throwable.generated.SSODeletionProblem
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.data.services.impls.keycloak.RealmValuesExistException
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigRetrieval
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
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
  fun retrieveSsoConfig(organizationId: UUID): SsoConfigRetrieval {
    val currentConfig = ssoConfigService.getSsoConfig(organizationId)
    if (currentConfig == null) {
      throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceId(organizationId.toString())
          .resourceType("sso_config"),
      )
    }

    try {
      val keycloakData = airbyteKeycloakClient.getSsoConfigData(organizationId, currentConfig.keycloakRealm)
      val domainData = organizationEmailDomainService.findByOrganizationId(organizationId).map { it.emailDomain }
      return SsoConfigRetrieval(
        companyIdentifier = currentConfig.keycloakRealm,
        clientId = keycloakData.clientId,
        clientSecret = keycloakData.clientSecret,
        emailDomains = domainData,
      )
    } catch (e: Exception) {
      throw SSOConfigRetrievalProblem(
        ProblemSSOConfigRetrievalData()
          .organizationId(organizationId.toString())
          .errorMessage("Error retrieving SSO config: $e"),
      )
    }
  }

  @Transactional("config")
  open fun createAndStoreSsoConfig(config: SsoConfig) {
    validateEmailDomain(config)
    validateExistingEmailDomainEntries(config)
    validateExistingSsoConfig(config)

    try {
      airbyteKeycloakClient.createOidcSsoConfig(config)
    } catch (ex: RealmValuesExistException) {
      throw BadRequestProblem(
        ProblemMessageData()
          .message("The provided company identifier is already associated with an SSO configuration: $ex"),
      )
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

  fun deleteSsoConfig(
    organizationId: UUID,
    companyIdentifier: String,
  ) {
    try {
      ssoConfigService.deleteSsoConfig(organizationId)
      organizationEmailDomainService.deleteAllEmailDomains(organizationId)
      airbyteKeycloakClient.deleteRealm(companyIdentifier)
    } catch (ex: Exception) {
      throw SSODeletionProblem(
        ProblemSSODeletionData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage(ex.message),
      )
    }
  }

  fun updateClientCredentials(clientConfig: SsoKeycloakIdpCredentials) {
    val currentSsoConfig = ssoConfigService.getSsoConfig(clientConfig.organizationId)
    if (currentSsoConfig == null) {
      throw SSOCredentialUpdateProblem(
        ProblemSSOCredentialUpdateData()
          .errorMessage("SSO Config does not exist for organization ${clientConfig.organizationId}"),
      )
    }

    try {
      airbyteKeycloakClient.updateIdpClientCredentials(clientConfig, currentSsoConfig.keycloakRealm)
    } catch (e: Exception) {
      throw SSOCredentialUpdateProblem(
        ProblemSSOCredentialUpdateData()
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage(e.message),
      )
    }
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
