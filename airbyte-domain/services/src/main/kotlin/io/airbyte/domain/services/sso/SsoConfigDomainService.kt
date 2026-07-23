/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.OrganizationDomainVerificationService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toDomain
import io.airbyte.data.services.impls.data.mappers.toSsoDefaultRole
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
import io.airbyte.domain.models.DEFAULT_SSO_ROLE
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigRetrieval
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.domain.models.SsoDefaultRole
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.featureflag.AutoGrantOrgPermissionsOnSsoActivation
import io.airbyte.featureflag.ConfigurableSsoDefaultRole
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseVerifiedDomainsForSsoActivate
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.net.URL
import java.util.UUID

private val logger = KotlinLogging.logger {}

// SSO_CONFIG_OPERATION metric "operation" tag values.
private const val SSO_OP_CREATE_DRAFT = "create_draft"
private const val SSO_OP_CREATE_ACTIVE = "create_active"
private const val SSO_OP_ACTIVATE = "activate"

@Singleton
open class SsoConfigDomainService internal constructor(
  private val ssoConfigService: SsoConfigService,
  private val organizationEmailDomainService: OrganizationEmailDomainService,
  private val airbyteKeycloakClient: AirbyteKeycloakClient,
  private val organizationService: OrganizationService,
  private val userPersistence: UserPersistence,
  private val organizationDomainVerificationService: OrganizationDomainVerificationService,
  private val featureFlagClient: FeatureFlagClient,
  private val permissionService: PermissionService,
  private val metricClient: MetricClient,
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
        defaultRole = currentConfig.defaultRole?.toSsoDefaultRole() ?: DEFAULT_SSO_ROLE,
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
        defaultRole = currentConfig.defaultRole?.toSsoDefaultRole() ?: DEFAULT_SSO_ROLE,
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
      SsoConfigStatus.DRAFT -> recordSsoConfigOperation(config.organizationId, SSO_OP_CREATE_DRAFT) { createDraftSsoConfig(config) }
      SsoConfigStatus.ACTIVE -> recordSsoConfigOperation(config.organizationId, SSO_OP_CREATE_ACTIVE) { createActiveSsoConfig(config) }
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
        // Only persist the role when the caller specified one. A null role means "unspecified",
        // so leave the stored role untouched rather than silently downgrading it to the default.
        config.defaultRole?.let { ssoConfigService.updateSsoConfigDefaultRole(config.organizationId, it) }
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
    // Read-only validation up front so we fail fast before creating any resources. The permission
    // grant is deferred into createActiveSsoConfigWithEmailDomain so it commits or rolls back
    // atomically with the SSO config and email domain, and only after the realm has been created.
    validateEmailDomainForActivationGated(
      organizationId = config.organizationId,
      emailDomain = configEmailDomain,
      companyIdentifier = config.companyIdentifier,
    )

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
      var cleanupSucceeded = true
      try {
        airbyteKeycloakClient.deleteRealm(config.companyIdentifier)
      } catch (cleanupEx: Exception) {
        cleanupSucceeded = false
        logger.error(cleanupEx) { "Failed to cleanup Keycloak realm after database failure" }
      }
      recordSetupCompensation(config.organizationId, cleanupSucceeded)
      throw ex
    }
  }

  /**
   * Creates an active SSO config, email domain, and any auto-granted permissions within a single
   * transaction to ensure atomicity.
   *
   * Transaction Boundary: This method IS marked @Transactional to ensure atomicity between creating
   * the SSO config database record, the email domain record, and the permission grants. All must
   * succeed or fail together. This is called after Keycloak resources are created, so we're only
   * transacting database operations here; the realm is compensated by the caller if this fails.
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
    // Grant permissions within this transaction so they roll back together with the config and
    // email-domain records on failure (no-op unless AutoGrantOrgPermissionsOnSsoActivation is on).
    grantPermissionsIfAutoGrantEnabled(
      organizationId = config.organizationId,
      emailDomain = configEmailDomain,
      defaultRole = config.defaultRole ?: DEFAULT_SSO_ROLE,
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

  /**
   * Grant the configured default SSO permission to a list of users for the specified organization.
   * This is used during SSO activation to grant access to existing users with the email domain.
   *
   * @param userIds list of user IDs to grant permissions to
   * @param organizationId the organization to grant permissions for
   */
  fun grantOrgMembershipToUsers(
    userIds: List<UUID>,
    organizationId: UUID,
    defaultRole: SsoDefaultRole = DEFAULT_SSO_ROLE,
  ) {
    if (userIds.isEmpty()) {
      return
    }

    val permissionType = defaultRole.toConfigModel()

    // Note: This is N+1 (one createPermission call per user). If this becomes a performance
    // issue, consider adding a batch createPermissions method to PermissionService.
    userIds.forEach { userId ->
      val permission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withOrganizationId(organizationId)
          .withPermissionType(permissionType)
      permissionService.createPermission(permission)
    }

    logger.info { "Granted $permissionType permission to ${userIds.size} users for organization $organizationId" }
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

  /**
   * Validates the email domain and grants permissions to users if the auto-grant flag is enabled.
   * When AutoGrantOrgPermissionsOnSsoActivation is ON, skips the blocking validation and instead
   * grants the configured default SSO permission to users with the email domain who don't have org access.
   * When OFF, uses the old behavior that blocks activation if users exist outside the org.
   *
   * This is a convenience for callers that are already inside a transaction (e.g. activateSsoConfig).
   * Callers that perform external (Keycloak) work first should instead run
   * [validateEmailDomainForActivationGated] up front and defer [grantPermissionsIfAutoGrantEnabled]
   * into their transaction, so a grant cannot be orphaned if a later step fails.
   */
  private fun validateEmailDomainAndGrantPermissionsIfNeeded(
    organizationId: UUID,
    emailDomain: String,
    companyIdentifier: String,
    defaultRole: SsoDefaultRole = DEFAULT_SSO_ROLE,
  ) {
    validateEmailDomainForActivationGated(organizationId, emailDomain, companyIdentifier)
    grantPermissionsIfAutoGrantEnabled(organizationId, emailDomain, defaultRole)
  }

  /**
   * Read-only email-domain validation for SSO activation, gated by AutoGrantOrgPermissionsOnSsoActivation.
   * When the flag is ON we skip the blocking "users outside the org" check (those users are granted access
   * instead, see [grantPermissionsIfAutoGrantEnabled]); when OFF we run the full blocking validation.
   * Performs no writes, so callers can run it up front to fail fast before creating any resources.
   */
  private fun validateEmailDomainForActivationGated(
    organizationId: UUID,
    emailDomain: String,
    companyIdentifier: String,
  ) {
    val autoGrantEnabled = featureFlagClient.boolVariation(AutoGrantOrgPermissionsOnSsoActivation, Organization(organizationId))

    if (autoGrantEnabled) {
      // NEW BEHAVIOR: Skip blocking validation, auto-grant permissions instead
      validateEmailDomainMatchesOrganization(organizationId, emailDomain, companyIdentifier)
      validateEmailDomainNotExists(emailDomain, organizationId, companyIdentifier)
      // Note: validateNoExistingUsersOutsideOrganization is SKIPPED
    } else {
      // OLD BEHAVIOR: Full validation including blocking on outside-org users
      validateEmailDomainForActivation(organizationId, emailDomain, companyIdentifier)
    }
  }

  /**
   * Grants the configured default role to users with the email domain who lack org access, but only when
   * AutoGrantOrgPermissionsOnSsoActivation is ON (otherwise a no-op). This performs writes and must run
   * inside the caller's transaction so the grants roll back with the rest of the SSO config persistence.
   */
  private fun grantPermissionsIfAutoGrantEnabled(
    organizationId: UUID,
    emailDomain: String,
    defaultRole: SsoDefaultRole = DEFAULT_SSO_ROLE,
  ) {
    val autoGrantEnabled = featureFlagClient.boolVariation(AutoGrantOrgPermissionsOnSsoActivation, Organization(organizationId))
    if (!autoGrantEnabled) {
      return
    }

    // Find and grant permissions to users who need them
    val usersNeedingPermission =
      userPersistence.findUsersWithEmailDomainWithoutOrgPermission(
        emailDomain,
        organizationId,
      )
    val effectiveRole = effectiveDefaultRole(organizationId, defaultRole)
    grantOrgMembershipToUsers(usersNeedingPermission, organizationId, effectiveRole)
    recordPermissionGrant(organizationId, usersNeedingPermission.size, effectiveRole)
  }

  /**
   * Resolves the SSO default role to apply during JIT provisioning, gated by
   * [ConfigurableSsoDefaultRole] (temporary, default OFF). While the flag is off for an
   * organization we ignore the configured role and fall back to [DEFAULT_SSO_ROLE], preserving
   * pre-feature behavior so the code can be deployed dark and released separately. Remove this
   * method and the flag once the configurable-default-role rollout completes.
   */
  private fun effectiveDefaultRole(
    organizationId: UUID,
    configured: SsoDefaultRole,
  ): SsoDefaultRole =
    if (featureFlagClient.boolVariation(ConfigurableSsoDefaultRole, Organization(organizationId))) {
      configured
    } else {
      DEFAULT_SSO_ROLE
    }

  open fun activateSsoConfig(
    organizationId: UUID,
    emailDomain: String? = null,
  ) {
    // The transactional work lives in activateSsoConfigTransactional. Because that method is open and
    // @Transactional, the Micronaut proxy commits (or rolls back) before it returns, so by the time
    // this lambda returns the outcome is final and the success/failure metric reflects the real commit
    // result. This mirrors the create path (createAndStoreSsoConfig wrapping the @Transactional
    // createActiveSsoConfigWithEmailDomain), which keeps the metric outside the transaction boundary.
    recordSsoConfigOperation(organizationId, SSO_OP_ACTIVATE) {
      activateSsoConfigTransactional(organizationId, emailDomain)
    }
  }

  @Transactional("config")
  internal open fun activateSsoConfigTransactional(
    organizationId: UUID,
    emailDomain: String?,
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

    // FeatureFlag check
    val useVerifiedDomains = featureFlagClient.boolVariation(UseVerifiedDomainsForSsoActivate, Organization(organizationId))

    if (useVerifiedDomains) {
      // fetching all the verified domains from the organizationDomainVerification table
      val verifiedDomains =
        organizationDomainVerificationService.findByOrganizationId(organizationId).filter {
          it.status ==
            DomainVerificationStatus.VERIFIED
        }

      if (verifiedDomains.isEmpty()) {
        throw SSOActivationProblem(
          ProblemSSOActivationData()
            .organizationId(organizationId)
            .companyIdentifier(currentSsoConfig.keycloakRealm)
            .errorMessage(
              "Cannot activate SSO: No verified domains found. Please verify at least one domain using DNS verification\n" +
                "   before activating SSO.",
            ),
        )
      }

      try {
        ssoConfigService.updateSsoConfigStatus(organizationId, SsoConfigStatus.ACTIVE)
        // create enforcement records for all verified domains
        verifiedDomains.forEach { verifiedDomain ->
          organizationEmailDomainService.createEmailDomain(
            OrganizationEmailDomain().apply {
              id = UUID.randomUUID()
              this.organizationId = organizationId
              this.emailDomain = verifiedDomain.domain
            },
          )
        }
      } catch (e: Exception) {
        logger.error(e) { "Failed to activate SSO config for organization $organizationId" }
        throw SSOActivationProblem(
          ProblemSSOActivationData()
            .organizationId(organizationId)
            .companyIdentifier(currentSsoConfig.keycloakRealm)
            .errorMessage("Failed to activate your SSO configuration. Please try again or contact support if the problem persists."),
        )
      }
    } else {
      // since now the email domain is not in required so need add check
      if (emailDomain.isNullOrEmpty()) {
        throw SSOActivationProblem(
          ProblemSSOActivationData()
            .organizationId(organizationId)
            .companyIdentifier(currentSsoConfig.keycloakRealm)
            .errorMessage("Email domain is required for SSO activation."),
        )
      }

      validateEmailDomainAndGrantPermissionsIfNeeded(
        organizationId,
        emailDomain,
        currentSsoConfig.keycloakRealm,
        currentSsoConfig.defaultRole?.toSsoDefaultRole() ?: DEFAULT_SSO_ROLE,
      )

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

  /**
   * Runs [block] and emits a single [OssMetricsRegistry.SSO_CONFIG_OPERATION] count tagged with the
   * [operation] and its success/failure status. Re-throws so transactional rollback is unaffected.
   */
  private fun <T> recordSsoConfigOperation(
    organizationId: UUID,
    operation: String,
    block: () -> T,
  ): T =
    try {
      val result = block()
      emitSsoConfigOperation(organizationId, operation, success = true)
      result
    } catch (ex: Exception) {
      emitSsoConfigOperation(organizationId, operation, success = false)
      throw ex
    }

  private fun emitSsoConfigOperation(
    organizationId: UUID,
    operation: String,
    success: Boolean,
  ) {
    try {
      metricClient.count(
        OssMetricsRegistry.SSO_CONFIG_OPERATION,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
            MetricAttribute(MetricTags.SSO_OPERATION, operation),
            MetricAttribute(MetricTags.STATUS, if (success) MetricTags.SUCCESS else MetricTags.FAILURE),
          ),
      )
    } catch (e: Exception) {
      logger.warn(e) { "Failed to emit SSO config operation metric for '$operation'" }
    }
  }

  /**
   * Emits [OssMetricsRegistry.SSO_SETUP_COMPENSATION] when an active-config setup fails and the
   * Keycloak realm has to be compensated, tagging whether that realm cleanup succeeded.
   */
  private fun recordSetupCompensation(
    organizationId: UUID,
    cleanupSucceeded: Boolean,
  ) {
    try {
      metricClient.count(
        OssMetricsRegistry.SSO_SETUP_COMPENSATION,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
            MetricAttribute(MetricTags.STATUS, if (cleanupSucceeded) MetricTags.SUCCESS else MetricTags.FAILURE),
          ),
      )
    } catch (e: Exception) {
      logger.warn(e) { "Failed to emit SSO setup compensation metric" }
    }
  }

  /**
   * Emits [OssMetricsRegistry.SSO_PERMISSION_GRANTED] with the number of users auto-granted an org
   * permission, tagged by the default role applied. No-op when nobody was granted.
   */
  private fun recordPermissionGrant(
    organizationId: UUID,
    grantedCount: Int,
    defaultRole: SsoDefaultRole,
  ) {
    if (grantedCount <= 0) {
      return
    }
    try {
      metricClient.count(
        OssMetricsRegistry.SSO_PERMISSION_GRANTED,
        grantedCount.toLong(),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.SSO_DEFAULT_ROLE, defaultRole.name),
      )
    } catch (e: Exception) {
      logger.warn(e) { "Failed to emit SSO permission granted metric" }
    }
  }
}
