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
import io.airbyte.data.services.impls.keycloak.IdpNotFoundException
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

    if (!airbyteKeycloakClient.realmExists(currentConfig.keycloakRealm)) {
      logger.error {
        "Keycloak realm '${currentConfig.keycloakRealm}' does not exist for organization $organizationId. Database and Keycloak are out of sync."
      }
      throw SSOConfigRetrievalProblem(
        ProblemSSOConfigRetrievalData()
          .organizationId(organizationId.toString())
          .errorMessage("Unable to retrieve your SSO configuration. Please contact support."),
      )
    }

    val domainData = organizationEmailDomainService.findByOrganizationId(organizationId).map { it.emailDomain }

    try {
      val keycloakData = airbyteKeycloakClient.getSsoConfigData(organizationId, currentConfig.keycloakRealm)
      return SsoConfigRetrieval(
        companyIdentifier = currentConfig.keycloakRealm,
        clientId = keycloakData.clientId,
        clientSecret = keycloakData.clientSecret,
        emailDomains = domainData,
        status = currentConfig.status.toDomain(),
      )
    } catch (e: IdpNotFoundException) {
      logger.warn(
        e,
      ) { "IDP not found for organization $organizationId in Keycloak realm ${currentConfig.keycloakRealm}, returning empty credentials" }
      return SsoConfigRetrieval(
        companyIdentifier = currentConfig.keycloakRealm,
        clientId = "",
        clientSecret = "",
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
   *
   * When updating a draft config, if the company identifier hasn't changed, the Keycloak realm is
   * preserved to maintain any existing user accounts. Only the IDP configuration is updated.
   *
   * Transaction Boundary: This method is NOT marked @Transactional because it performs external
   * Keycloak operations that cannot be rolled back via database transactions. Instead, we create
   * Keycloak resources first, then database records. If database operations fail, we manually
   * clean up the Keycloak resources. This ensures proper cleanup without holding database
   * transactions open during external API calls.
   */
  open fun createAndStoreSsoConfig(config: SsoConfig) {
    when (config.status) {
      SsoConfigStatus.DRAFT -> createDraftSsoConfig(config)
      SsoConfigStatus.ACTIVE -> createActiveSsoConfig(config)
    }
  }

  /**
   * Creates a draft SSO config. Validates the config, handles existing configs, creates the Keycloak
   * realm first, then the database record. If database operations fail, deletes the Keycloak realm
   * that was just created.
   *
   * When updating a draft config with the same company identifier, preserves the Keycloak realm and
   * users by only updating the IDP configuration. If the realm doesn't exist but the DB record does,
   * recreates the realm.
   */
  private fun createDraftSsoConfig(config: SsoConfig) {
    validateDiscoveryUrl(config)

    if (config.emailDomain != null) {
      throw BadRequestProblem(
        ProblemMessageData()
          .message("Email domain should not be provided when creating a draft SSO config"),
      )
    }

    val existingConfig = ssoConfigService.getSsoConfig(config.organizationId)
    if (existingConfig != null && existingConfig.status.toDomain() == SsoConfigStatus.ACTIVE) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("An active SSO Config already exists for organization ${config.organizationId}"),
      )
    }

    when {
      existingConfig == null -> createNewDraftSsoConfig(config)

      existingConfig.keycloakRealm != config.companyIdentifier -> {
        deleteSsoConfig(config.organizationId, existingConfig.keycloakRealm)
        createNewDraftSsoConfig(config)
      }

      airbyteKeycloakClient.realmExists(config.companyIdentifier) -> {
        updateExistingKeycloakRealmConfig(config)
      }

      else -> {
        logger.info {
          "Realm ${config.companyIdentifier} does not exist but DB record does for organization ${config.organizationId}, recreating realm"
        }
        createKeycloakRealmWithErrorHandling(config)
      }
    }
  }

  private fun createNewDraftSsoConfig(config: SsoConfig) {
    createKeycloakRealmWithErrorHandling(config)
    try {
      ssoConfigService.createSsoConfig(config)
    } catch (ex: Exception) {
      try {
        airbyteKeycloakClient.deleteRealm(config.companyIdentifier)
      } catch (cleanupEx: Exception) {
        logger.error(cleanupEx) { "Failed to cleanup Keycloak realm after database failure" }
      }
      throw ex
    }
  }

  private fun createKeycloakRealmWithErrorHandling(config: SsoConfig) {
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
  }

  /**
   * Creates an active SSO config. Validates the config, handles existing configs, creates the Keycloak
   * realm first, then the database record and email domain.
   */
  private fun createActiveSsoConfig(config: SsoConfig) {
    validateDiscoveryUrl(config)

    val configEmailDomain =
      config.emailDomain ?: throw BadRequestProblem(
        ProblemMessageData()
          .message("Email domain is required when creating an active SSO config"),
      )
    validateEmailDomainMatchesOrganization(config.organizationId, configEmailDomain, config.companyIdentifier)
    validateExistingEmailDomainEntries(config)

    val existingConfig = ssoConfigService.getSsoConfig(config.organizationId)
    if (existingConfig != null) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "An SSO Config already exists for organization ${config.organizationId}. Either activate the existing draft, or delete it before attempting to create a new ACTIVE SSO Config.",
          ),
      )
    }

    createKeycloakRealmWithErrorHandling(config)
    try {
      createActiveSsoConfigWithEmailDomain(config)
    } catch (ex: Exception) {
      try {
        airbyteKeycloakClient.deleteRealm(config.companyIdentifier)
      } catch (cleanupEx: Exception) {
        logger.error(cleanupEx) { "Failed to cleanup Keycloak realm after database failure" }
      }
      throw ex
    }
  }

  /**
   * Creates an active SSO config and email domain within a single transaction to ensure atomicity.
   *
   * Transaction Boundary: This method IS marked @Transactional to ensure atomicity between
   * creating the SSO config database record and the email domain record. Both must succeed or
   * fail together. This is called after Keycloak resources are created, so we're only transacting
   * database operations here.
   */
  @Transactional("config")
  internal open fun createActiveSsoConfigWithEmailDomain(config: SsoConfig) {
    val configEmailDomain =
      config.emailDomain ?: throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "Failed to create an active SSO Config because emailDomain is null",
          ),
      )
    ssoConfigService.createSsoConfig(config)
    organizationEmailDomainService.createEmailDomain(
      OrganizationEmailDomain().apply {
        id = UUID.randomUUID()
        this.organizationId = config.organizationId
        this.emailDomain = configEmailDomain
      },
    )
  }

  /**
   * Updates an existing draft SSO config when the company identifier hasn't changed.
   * This preserves the Keycloak realm and any existing users, only updating the IDP configuration.
   */
  private fun updateExistingKeycloakRealmConfig(config: SsoConfig) {
    try {
      airbyteKeycloakClient.replaceOidcIdpConfig(config)
    } catch (ex: Exception) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Failed to replace IDP configuration: ${ex.message}"),
      )
    }
  }

  fun deleteSsoConfig(
    organizationId: UUID,
    companyIdentifier: String,
  ) {
    val existingConfig = ssoConfigService.getSsoConfig(organizationId)

    if (existingConfig == null) {
      throw SSODeletionProblem(
        ProblemSSODeletionData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage("No SSO config found for organization $organizationId"),
      )
    }

    if (existingConfig.keycloakRealm != companyIdentifier) {
      throw SSODeletionProblem(
        ProblemSSODeletionData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage(
            "Company identifier mismatch: provided '$companyIdentifier' does not match DB realm '${existingConfig.keycloakRealm}' for organization $organizationId",
          ),
      )
    }

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

    if (!airbyteKeycloakClient.realmExists(currentSsoConfig.keycloakRealm)) {
      throw SSOCredentialUpdateProblem(
        ProblemSSOCredentialUpdateData()
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage("Keycloak realm '${currentSsoConfig.keycloakRealm}' does not exist"),
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

  @InternalForTesting
  internal fun validateEmailDomainMatchesOrganization(
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
      organizationEmailDomainService.createEmailDomain(
        OrganizationEmailDomain().apply {
          id = UUID.randomUUID()
          this.organizationId = organizationId
          this.emailDomain = emailDomain
        },
      )
    } catch (e: Exception) {
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage("Error activating SSO config: ${e.message}"),
      )
    }
  }
}
