/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.model.generated.ProblemSSOActivationData
import io.airbyte.api.problems.model.generated.ProblemSSOConfigRetrievalData
import io.airbyte.api.problems.model.generated.ProblemSSOCredentialUpdateData
import io.airbyte.api.problems.model.generated.ProblemSSODeletionData
import io.airbyte.api.problems.model.generated.ProblemSSOSetupData
import io.airbyte.api.problems.model.generated.ProblemSSOTokenValidationData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.SSOActivationProblem
import io.airbyte.api.problems.throwable.generated.SSOConfigRetrievalProblem
import io.airbyte.api.problems.throwable.generated.SSOCredentialUpdateProblem
import io.airbyte.api.problems.throwable.generated.SSODeletionProblem
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.api.problems.throwable.generated.SSOTokenValidationProblem
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.data.mappers.toDomain
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.data.services.impls.keycloak.CreateClientException
import io.airbyte.data.services.impls.keycloak.IdpCreationException
import io.airbyte.data.services.impls.keycloak.IdpNotFoundException
import io.airbyte.data.services.impls.keycloak.ImportConfigException
import io.airbyte.data.services.impls.keycloak.InvalidOidcDiscoveryDocumentException
import io.airbyte.data.services.impls.keycloak.InvalidTokenException
import io.airbyte.data.services.impls.keycloak.KeycloakServiceException
import io.airbyte.data.services.impls.keycloak.MalformedTokenResponseException
import io.airbyte.data.services.impls.keycloak.RealmCreationException
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
  private val userPersistence: UserPersistence,
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
      logger.error(e) { "Failed to retrieve SSO config data for organization $organizationId from Keycloak realm ${currentConfig.keycloakRealm}" }
      throw SSOConfigRetrievalProblem(
        ProblemSSOConfigRetrievalData()
          .organizationId(organizationId.toString())
          .errorMessage("Failed to retrieve your SSO configuration. Please try again or contact support if the problem persists."),
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
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Email domain should not be provided when creating a draft SSO configuration. It will be required during activation."),
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
      createSsoConfigIfIdentifierUnused(config)
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
      logger.error(ex) { "Realm values already exist for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("This company identifier is already in use. Please choose a different identifier."),
      )
    } catch (ex: RealmCreationException) {
      logger.error(ex) { "Failed to create Keycloak realm for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Failed to create the authentication realm. Please try again or contact support if the problem persists."),
      )
    } catch (ex: InvalidOidcDiscoveryDocumentException) {
      logger.error(ex) {
        "Imported OIDC discovery document is missing required fields ${ex.missingFields} for company identifier ${config.companyIdentifier}"
      }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("The discovery URL did not return valid SSO configuration. Please verify your identity provider's discovery URL"),
      )
    } catch (ex: ImportConfigException) {
      logger.error(ex) { "Failed to import OIDC configuration from discovery URL for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "Unable to access or validate the OIDC discovery URL. Please verify the URL is correct and accessible from your identity provider's documentation.",
          ),
      )
    } catch (ex: IdpCreationException) {
      logger.error(ex) { "Failed to create IDP for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Failed to configure your identity provider. Please verify your Client ID and Client Secret are correct."),
      )
    } catch (ex: CreateClientException) {
      logger.error(ex) { "Failed to create Keycloak client for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Failed to complete the SSO configuration. Please try again."),
      )
    } catch (ex: Exception) {
      logger.error(ex) { "Unexpected error during SSO setup for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("An unexpected error occurred during SSO setup. Please try again or contact support if the problem persists."),
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
      config.emailDomain ?: throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("An email domain is required when creating an active SSO configuration."),
      )
    validateEmailDomainForActivation(config.organizationId, configEmailDomain, config.companyIdentifier)

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
    createSsoConfigIfIdentifierUnused(config)
    organizationEmailDomainService.createEmailDomain(
      OrganizationEmailDomain().apply {
        id = UUID.randomUUID()
        this.organizationId = config.organizationId
        this.emailDomain = configEmailDomain
      },
    )
  }

  private fun createSsoConfigIfIdentifierUnused(config: SsoConfig) {
    val existingConfig = ssoConfigService.getSsoConfigByCompanyIdentifier(config.companyIdentifier)
    if (existingConfig != null) {
      logger.error { "An SsoConfig already exists for ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "This company identifier is already in use. Please choose a different identifier",
          ),
      )
    }
    try {
      ssoConfigService.createSsoConfig(config)
    } catch (ex: Exception) {
      logger.error(ex) { "Failed to create SsoConfig for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "Failed to save your SSO configuration. Please try again or contact support if the problem persists.",
          ),
      )
    }
  }

  /**
   * Updates an existing draft SSO config when the company identifier hasn't changed.
   * This preserves the Keycloak realm and any existing users, only updating the IDP configuration.
   */
  private fun updateExistingKeycloakRealmConfig(config: SsoConfig) {
    try {
      airbyteKeycloakClient.replaceOidcIdpConfig(config)
    } catch (ex: ImportConfigException) {
      logger.error(ex) { "Failed to import OIDC configuration from discovery URL for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "Unable to access or validate the OIDC discovery URL. Please verify the URL is correct and accessible from your identity provider's documentation.",
          ),
      )
    } catch (ex: IdpCreationException) {
      logger.error(ex) { "Failed to update IDP for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("Failed to update your identity provider configuration. Please verify your Client ID and Client Secret are correct."),
      )
    } catch (ex: Exception) {
      logger.error(ex) { "Unexpected error updating IDP configuration for company identifier ${config.companyIdentifier}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage(
            "An unexpected error occurred while updating your identity provider. Please try again or contact support if the problem persists.",
          ),
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
      logger.error(ex) { "Failed to delete SSO configuration for organization $organizationId with realm $companyIdentifier" }
      throw SSODeletionProblem(
        ProblemSSODeletionData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage("Failed to delete your SSO configuration. Please try again or contact support if the problem persists."),
      )
    }
  }

  fun updateClientCredentials(clientConfig: SsoKeycloakIdpCredentials) {
    val currentSsoConfig =
      ssoConfigService.getSsoConfig(clientConfig.organizationId) ?: throw SSOCredentialUpdateProblem(
        ProblemSSOCredentialUpdateData()
          .companyIdentifier("")
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
      logger.error(e) {
        "Failed to update IDP client credentials for organization ${clientConfig.organizationId} in realm ${currentSsoConfig.keycloakRealm}"
      }
      throw SSOCredentialUpdateProblem(
        ProblemSSOCredentialUpdateData()
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage("Failed to update your identity provider credentials. Please verify your Client ID and Client Secret are correct."),
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
          .errorMessage(
            "The access token was issued by a different realm than expected. Expected: ${ssoConfig.keycloakRealm}, Actual: ${tokenRealm ?: "unknown"}",
          ),
      )
    }

    // Now validate the token against the organization's specific realm
    try {
      airbyteKeycloakClient.validateTokenWithRealm(accessToken, ssoConfig.keycloakRealm)
    } catch (e: TokenExpiredException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("The access token has expired. Please try testing your SSO configuration again."),
      )
    } catch (e: InvalidTokenException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("The access token format is invalid or malformed."),
      )
    } catch (e: MalformedTokenResponseException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Failed to validate the token with the identity provider. This may indicate incorrect client credentials."),
      )
    } catch (e: KeycloakServiceException) {
      throw SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(organizationId)
          .errorMessage("Failed to communicate with the identity provider (service unavailable)."),
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
      throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
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
      throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage("The provided email domain '$emailDomain' does not match your organization's email domain '${orgEmailDomain ?: "unknown"}'."),
      )
    }
  }

  private fun validateEmailDomainNotExists(
    emailDomain: String,
    organizationId: UUID,
    companyIdentifier: String,
  ) {
    val existingEmailDomains = organizationEmailDomainService.findByEmailDomain(emailDomain)
    if (existingEmailDomains.isNotEmpty()) {
      throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage("This email domain '$emailDomain' is already associated with another organization's SSO configuration."),
      )
    }
  }

  private fun validateNoExistingUsersOutsideOrganization(
    emailDomain: String,
    organizationId: UUID,
    companyIdentifier: String,
  ) {
    val usersOutsideOrg = userPersistence.findUsersWithEmailDomainOutsideOrganization(emailDomain, organizationId)
    if (usersOutsideOrg.isNotEmpty()) {
      throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
          .companyIdentifier(companyIdentifier)
          .errorMessage(
            "Cannot activate SSO for domain '$emailDomain' because ${usersOutsideOrg.size} user(s) from other organizations are already using this " +
              "domain. Please contact support to resolve this conflict.",
          ),
      )
    }
  }

  private fun validateDiscoveryUrl(config: SsoConfig) {
    try {
      URL(config.discoveryUrl)
    } catch (e: Exception) {
      logger.error(e) { "Invalid discovery URL format for config with company identifier ${config.companyIdentifier}: ${config.discoveryUrl}" }
      throw SSOSetupProblem(
        ProblemSSOSetupData()
          .companyIdentifier(config.companyIdentifier)
          .errorMessage("The discovery URL format is invalid. Please verify your identity provider's discovery URL and try again."),
      )
    }
  }

  private fun validateEmailDomainForActivation(
    organizationId: UUID,
    emailDomain: String,
    companyIdentifier: String,
  ) {
    validateEmailDomainMatchesOrganization(organizationId, emailDomain, companyIdentifier)
    validateEmailDomainNotExists(emailDomain, organizationId, companyIdentifier)
    validateNoExistingUsersOutsideOrganization(emailDomain, organizationId, companyIdentifier)
  }

  @Transactional("config")
  open fun activateSsoConfig(
    organizationId: UUID,
    emailDomain: String,
  ) {
    val currentSsoConfig =
      ssoConfigService.getSsoConfig(organizationId) ?: throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
          .errorMessage("No SSO configuration exists for this organization. Please create one first."),
      )
    if (currentSsoConfig.status.toDomain() == SsoConfigStatus.ACTIVE) {
      throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage("This SSO configuration is already active."),
      )
    }
    validateEmailDomainForActivation(organizationId, emailDomain, currentSsoConfig.keycloakRealm)

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
      logger.error(e) { "Failed to activate SSO config for organization $organizationId with email domain $emailDomain" }
      throw SSOActivationProblem(
        ProblemSSOActivationData()
          .organizationId(organizationId)
          .companyIdentifier(currentSsoConfig.keycloakRealm)
          .errorMessage("Failed to activate your SSO configuration. Please try again or contact support if the problem persists."),
      )
    }
  }
}
