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
import io.airbyte.api.problems.model.generated.ProblemSSOTokenValidationData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.SSOConfigRetrievalProblem
import io.airbyte.api.problems.throwable.generated.SSOCredentialUpdateProblem
import io.airbyte.api.problems.throwable.generated.SSODeletionProblem
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.api.problems.throwable.generated.SSOTokenValidationProblem
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.data.mappers.toDomain
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.data.services.impls.keycloak.InvalidTokenException
import io.airbyte.data.services.impls.keycloak.KeycloakServiceException
import io.airbyte.data.services.impls.keycloak.MalformedTokenResponseException
import io.airbyte.data.services.impls.keycloak.RealmDeletionException
import io.airbyte.data.services.impls.keycloak.RealmValuesExistException
import io.airbyte.data.services.impls.keycloak.TokenExpiredException
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigRetrieval
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.net.URL
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
open class SsoConfigDomainService internal constructor(
  private val ssoConfigService: SsoConfigService,
  private val organizationEmailDomainService: OrganizationEmailDomainService,
  private val airbyteKeycloakClient: AirbyteKeycloakClient,
  private val organizationService: OrganizationService,
) {
  fun retrieveSsoConfig(organizationId: UUID): SsoConfigRetrieval {
    val currentConfig =
      ssoConfigService.getSsoConfig(organizationId) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceId(organizationId.toString())
          .resourceType("sso_config"),
      )
    try {
      val keycloakData = airbyteKeycloakClient.getSsoConfigData(organizationId, currentConfig.keycloakRealm)
      val domainData = organizationEmailDomainService.findByOrganizationId(organizationId).map { it.emailDomain }
      return SsoConfigRetrieval(
        companyIdentifier = currentConfig.keycloakRealm,
        clientId = keycloakData.clientId,
        clientSecret = keycloakData.clientSecret,
        emailDomains = domainData,
        status = currentConfig.status.toDomain(),
      )
    } catch (e: Exception) {
      throw SSOConfigRetrievalProblem(
        ProblemSSOConfigRetrievalData()
          .organizationId(organizationId.toString())
          .errorMessage("Error retrieving SSO config: $e"),
      )
    }
  }

  /**
   * An SSO Config can be created in DRAFT or ACTIVE status. If the config is created in DRAFT status,
   * the email domain should not be provided up front. It will be provided later when the draft
   * config is activated. If the config is created in ACTIVE status, the email domain is required
   * and will be used to enforce SSO logins for the organization immediately.
   */
  @Transactional("config")
  open fun createAndStoreSsoConfig(config: SsoConfig) {
    validateDiscoveryUrl(config)
    validateEmailDomain(config)

    val existingConfig = ssoConfigService.getSsoConfig(config.organizationId)
    if (existingConfig != null) {
      if (existingConfig.status.toDomain() == SsoConfigStatus.DRAFT) {
        deleteSsoConfig(config.organizationId, existingConfig.keycloakRealm)
      } else {
        throw SSOSetupProblem(
          ProblemSSOSetupData()
            .companyIdentifier(config.companyIdentifier)
            .errorMessage("An active SSO Config already exists for organization ${config.organizationId}"),
        )
      }
    }

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

    if (config.status == SsoConfigStatus.ACTIVE) {
      createEmailDomainRecord(config.organizationId, config.emailDomain!!)
    }
  }

  fun deleteSsoConfig(
    organizationId: UUID,
    companyIdentifier: String,
  ) {
    try {
      ssoConfigService.deleteSsoConfig(organizationId)
      organizationEmailDomainService.deleteAllEmailDomains(organizationId)
      airbyteKeycloakClient.deleteRealm(companyIdentifier)
    } catch (ex: RealmDeletionException) {
      logger.warn(ex) { "Ignoring realm deletion exception because the realm might not exist " }
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
    val currentSsoConfig =
      ssoConfigService.getSsoConfig(clientConfig.organizationId) ?: throw SSOCredentialUpdateProblem(
        ProblemSSOCredentialUpdateData()
          .errorMessage("SSO Config does not exist for organization ${clientConfig.organizationId}"),
      )
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

  fun validateToken(
    organizationId: UUID,
    accessToken: String,
  ) {
    // First, retrieve the organization's SSO configuration
    val ssoConfig =
      ssoConfigService.getSsoConfig(organizationId) ?: throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("SSO configuration does not exist for organization $organizationId"),
      )

    // Extract the realm from the token
    val tokenRealm = airbyteKeycloakClient.extractRealmFromToken(accessToken)

    // Verify the token's realm matches the organization's configured realm
    if (tokenRealm == null || tokenRealm != ssoConfig.keycloakRealm) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Token does not belong to organization realm ${ssoConfig.keycloakRealm}"),
      )
    }

    // Now validate the token against the organization's specific realm
    try {
      airbyteKeycloakClient.validateTokenWithRealm(accessToken, ssoConfig.keycloakRealm)
    } catch (e: TokenExpiredException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Token is expired or invalid"),
      )
    } catch (e: InvalidTokenException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Token is invalid: ${e.message}"),
      )
    } catch (e: MalformedTokenResponseException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Token validation failed: ${e.message}"),
      )
    } catch (e: KeycloakServiceException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Unable to validate token: Keycloak service unavailable"),
      )
    }
  }

  /**
   * Validates email domain requirements based on SSO config status.
   *
   * For ACTIVE configs:
   * - Email domain is required
   * - Email domain must match the organization's email domain
   * - Email domain must not already exist in another organization
   *
   * For DRAFT configs:
   * - Email domain must not be provided (will be required later during config activation)
   */
  @InternalForTesting
  internal fun validateEmailDomain(config: SsoConfig) {
    val configEmailDomain = config.emailDomain
    when (config.status) {
      SsoConfigStatus.ACTIVE -> {
        if (configEmailDomain == null) {
          throw BadRequestProblem(
            ProblemMessageData()
              .message("Email domain is required when creating an active SSO config"),
          )
        }
        validateEmailDomainMatchesOrganization(config.organizationId, configEmailDomain, config.companyIdentifier)
        validateExistingEmailDomainEntries(config)
      }
      SsoConfigStatus.DRAFT -> {
        if (config.emailDomain != null) {
          throw BadRequestProblem(
            ProblemMessageData()
              .message("Email domain should not be provided when creating a draft SSO config"),
          )
        }
      }
    }
  }

  private fun validateEmailDomainMatchesOrganization(
    organizationId: UUID,
    emailDomain: String,
    companyIdentifier: String,
  ) {
    val org = organizationService.getOrganization(organizationId)
    if (org.isEmpty) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(companyIdentifier)
          .errorMessage("Organization with id $organizationId not found"),
      )
    }
    val orgEmailDomain =
      org
        .get()
        .email
        .split("@")
        .getOrNull(1)
    if (orgEmailDomain == null || orgEmailDomain != emailDomain) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(companyIdentifier)
          .errorMessage("Invalid email domain: $orgEmailDomain. Domain must match the organization"),
      )
    }
  }

  private fun validateExistingEmailDomainEntries(config: SsoConfig) {
    val emailDomain =
      config.emailDomain ?: throw BadRequestProblem(
        ProblemMessageData()
          .message("Email domain is required when validating existing email domain entries"),
      )
    validateEmailDomainNotExists(emailDomain, config.companyIdentifier)
  }

  private fun validateEmailDomainNotExists(
    emailDomain: String,
    companyIdentifier: String,
  ) {
    val existingEmailDomains = organizationEmailDomainService.findByEmailDomain(emailDomain)
    if (existingEmailDomains.isNotEmpty()) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(companyIdentifier)
          .errorMessage("Email domain already exists: $emailDomain"),
      )
    }
  }

  private fun createEmailDomainRecord(
    organizationId: UUID,
    emailDomain: String,
  ) {
    organizationEmailDomainService.createEmailDomain(
      OrganizationEmailDomain().apply {
        id = UUID.randomUUID()
        this.organizationId = organizationId
        this.emailDomain = emailDomain
      },
    )
  }

  private fun validateDiscoveryUrl(config: SsoConfig) {
    try {
      URL(config.discoveryUrl)
    } catch (e: Exception) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Provided discoveryUrl is not valid: ${e.message}"),
      )
    }
  }

  @Transactional("config")
  open fun activateSsoConfig(
    organizationId: UUID,
    emailDomain: String,
  ) {
    val currentSsoConfig =
      ssoConfigService.getSsoConfig(organizationId) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceId(organizationId.toString())
          .resourceType("sso_config"),
      )
    if (currentSsoConfig.status.toDomain() == SsoConfigStatus.ACTIVE) {
      throw BadRequestProblem(
        ProblemMessageData()
          .message("SSO config is already active for organization $organizationId"),
      )
    }
    validateEmailDomainMatchesOrganization(organizationId, emailDomain, currentSsoConfig.keycloakRealm)
    validateEmailDomainNotExists(emailDomain, currentSsoConfig.keycloakRealm)

    try {
      ssoConfigService.updateSsoConfigStatus(organizationId, SsoConfigStatus.ACTIVE)
      createEmailDomainRecord(organizationId, emailDomain)
    } catch (e: Exception) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage("Error activating SSO config: ${e.message}"),
      )
    }
  }
}
