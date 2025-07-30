/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.AuthConfiguration
import io.airbyte.api.model.generated.InstanceConfigurationResponse
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum
import io.airbyte.api.model.generated.InstanceConfigurationResponse.TrackingStrategyEnum
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody
import io.airbyte.api.model.generated.LicenseInfoResponse
import io.airbyte.api.model.generated.LicenseStatus
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.commons.auth.config.AuthConfigs
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.config.GenericOidcConfig
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.AirbyteLicense
import io.airbyte.commons.server.helpers.KubernetesClientPermissionHelper
import io.airbyte.commons.server.helpers.PermissionDeniedException
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Organization
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.time.Clock
import java.util.Date
import java.util.Optional
import java.util.UUID

/**
 * InstanceConfigurationHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
open class InstanceConfigurationHandler(
  @param:Named("airbyteUrl") private val airbyteUrl: Optional<String>,
  @param:Value("\${airbyte.tracking.strategy:}") private val trackingStrategy: String,
  private val airbyteEdition: AirbyteEdition,
  private val airbyteVersion: AirbyteVersion,
  private val activeAirbyteLicense: Optional<ActiveAirbyteLicense>,
  private val workspacePersistence: WorkspacePersistence,
  private val workspacesHandler: WorkspacesHandler,
  private val userPersistence: UserPersistence,
  private val organizationPersistence: OrganizationPersistence,
  private val authConfigs: AuthConfigs,
  private val permissionHandler: PermissionHandler,
  clock: Optional<Clock>,
  private val oidcEndpointConfig: Optional<GenericOidcConfig>,
  private val kubernetesClientPermissionHelper: Optional<KubernetesClientPermissionHelper>,
) {
  private val clock: Clock = clock.orElse(Clock.systemUTC())

  @get:Throws(IOException::class)
  val instanceConfiguration: InstanceConfigurationResponse
    get() {
      val defaultOrganization = defaultOrganization
      val initialSetupComplete = workspacePersistence.getInitialSetupComplete()

      return InstanceConfigurationResponse()
        .airbyteUrl(airbyteUrl.orElse("airbyte-url-not-configured"))
        .edition(airbyteEdition.convertTo<EditionEnum>())
        .version(airbyteVersion.serialize())
        .licenseStatus(currentLicenseStatus())
        .licenseExpirationDate(licenseExpirationDate())
        .auth(authConfiguration)
        .initialSetupComplete(initialSetupComplete)
        .defaultUserId(defaultUserId)
        .defaultOrganizationId(defaultOrganization.organizationId)
        .defaultOrganizationEmail(defaultOrganization.email)
        .trackingStrategy(
          if ("segment".equals(
              trackingStrategy,
              ignoreCase = true,
            )
          ) {
            TrackingStrategyEnum.SEGMENT
          } else {
            TrackingStrategyEnum.LOGGING
          },
        )
    }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun setupInstanceConfiguration(requestBody: InstanceConfigurationSetupRequestBody): InstanceConfigurationResponse {
    val defaultOrganization = defaultOrganization
    val defaultWorkspace = getDefaultWorkspace(defaultOrganization.organizationId)

    // Update the default organization and user with the provided information.
    // note that this is important especially for Community edition w/ Auth enabled,
    // because the login email must match the default organization's saved email in
    // order to login successfully.
    updateDefaultOrganization(requestBody)

    updateDefaultUser(requestBody)

    // Update the underlying workspace to mark the initial setup as complete
    workspacesHandler.updateWorkspace(
      WorkspaceUpdate()
        .workspaceId(defaultWorkspace.workspaceId)
        .email(requestBody.email)
        .displaySetupWizard(requestBody.displaySetupWizard)
        .anonymousDataCollection(requestBody.anonymousDataCollection)
        .initialSetupComplete(requestBody.initialSetupComplete),
    )

    // Return the updated instance configuration
    return instanceConfiguration
  }

  private val authConfiguration: AuthConfiguration
    get() {
      val authConfig =
        AuthConfiguration().mode(
          authConfigs.authMode?.convertTo<AuthConfiguration.ModeEnum>(),
        )

      // if Enterprise configurations are present, set OIDC-specific configs
      if (authConfigs.authMode == AuthMode.OIDC) {
        if (oidcEndpointConfig.isPresent) {
          authConfig.authorizationServerUrl = oidcEndpointConfig.get().authorizationServerEndpoint
          authConfig.clientId = oidcEndpointConfig.get().clientId
          if (oidcEndpointConfig.get().audience != null) {
            authConfig.audience = oidcEndpointConfig.get().audience
          }
        } else if (authConfigs.keycloakConfig != null && airbyteUrl.isPresent) {
          authConfig.clientId = authConfigs.keycloakConfig!!.webClientId
          authConfig.authorizationServerUrl = airbyteUrl.get() + "/auth/realms/" + authConfigs.keycloakConfig!!.airbyteRealm
        } else {
          // TODO: This is a bad error message. Once we figure out what the final config should look like
          throw IllegalStateException("OIDC must be configured either for Keycloak or in generic oidc mode.")
        }
      }

      return authConfig
    }

  @get:Throws(IOException::class)
  private val defaultUserId: UUID
    get() =
      userPersistence
        .getDefaultUser()
        .orElseThrow {
          IllegalStateException("Default user does not exist.")
        }.userId

  @Throws(IOException::class)
  private fun updateDefaultUser(requestBody: InstanceConfigurationSetupRequestBody) {
    val defaultUser =
      userPersistence.getDefaultUser().orElseThrow { IllegalStateException("Default user does not exist.") }

    // If a user with the provided email already exists (which can be the case in Enterprise), we should
    // not update the default user's email to the provided email since it would cause a conflict.
    val existingUserWithEmail = userPersistence.getUserByEmail(requestBody.email)
    if (existingUserWithEmail.isPresent) {
      log.info(
        "User ID {} already has the provided email {}. Not updating default user's email.",
        existingUserWithEmail.get().userId,
        requestBody.email,
      )
    } else {
      defaultUser.email = requestBody.email
    }

    // name is currently optional, so only set it if it is provided.
    if (requestBody.userName != null) {
      defaultUser.name = requestBody.userName
    }

    userPersistence.writeAuthenticatedUser(defaultUser)
  }

  @get:Throws(IOException::class)
  private val defaultOrganization: Organization
    get() =
      organizationPersistence.defaultOrganization
        .orElseThrow {
          IllegalStateException(
            "Default organization does not exist.",
          )
        }

  @Throws(IOException::class)
  private fun updateDefaultOrganization(requestBody: InstanceConfigurationSetupRequestBody) {
    val defaultOrganization =
      organizationPersistence.defaultOrganization.orElseThrow { IllegalStateException("Default organization does not exist.") }

    // email is a required request property, so always set it.
    defaultOrganization.email = requestBody.email

    // name is currently optional, so only set it if it is provided.
    if (requestBody.organizationName != null) {
      defaultOrganization.name = requestBody.organizationName
    }

    organizationPersistence.updateOrganization(defaultOrganization)
  }

  // Historically, instance setup for an OSS installation of Airbyte was stored on the one and only
  // workspace that was created for the instance. Now that OSS supports multiple workspaces, we
  // use the default Organization ID to select a workspace to use for instance setup. This is a hack.
  // TODO persist instance configuration to a separate resource, rather than using a workspace.
  @Throws(IOException::class)
  private fun getDefaultWorkspace(organizationId: UUID): StandardWorkspace = workspacePersistence.getDefaultWorkspaceForOrganization(organizationId)

  fun licenseInfo(): LicenseInfoResponse? {
    val license = activeAirbyteLicense.map(ActiveAirbyteLicense::license).orElse(null)
    if (license != null) {
      return LicenseInfoResponse()
        .edition(license.type.toString())
        .expirationDate(licenseExpirationDate())
        .usedEditors(editorsUsage())
        .maxEditors(license.maxEditors)
        .maxNodes(license.maxNodes)
        .usedNodes(nodesUsage())
        .licenseStatus(currentLicenseStatus())
    }
    return null
  }

  private fun licenseExpirationDate(): Long? {
    val license = activeAirbyteLicense.map(ActiveAirbyteLicense::license).orElse(null)

    if (license != null) {
      val expDate = license.expirationDate
      if (expDate != null) {
        return expDate.toInstant().toEpochMilli() / 1000
      }
    }
    return null
  }

  private fun editorsUsage(): Int = permissionHandler.countInstanceEditors()

  @VisibleForTesting
  fun currentLicenseStatus(): LicenseStatus? {
    if (activeAirbyteLicense.isEmpty) {
      return null
    }
    if (activeAirbyteLicense.get().license == null ||
      activeAirbyteLicense.get().license!!.type == AirbyteLicense.LicenseType.INVALID ||
      activeAirbyteLicense.get().license!!.type == AirbyteLicense.LicenseType.PRO
    ) {
      return LicenseStatus.INVALID
    }
    val actualLicense = activeAirbyteLicense.get().license
    if (Optional
        .ofNullable(actualLicense!!.expirationDate)
        .map { exp: Date ->
          exp.toInstant().isBefore(clock.instant())
        }.orElse(false)
    ) {
      return LicenseStatus.EXPIRED
    }
    if (Optional.ofNullable(actualLicense.maxEditors).map { m: Int -> editorsUsage() > m }.orElse(false)) {
      return LicenseStatus.EXCEEDED
    }
    return LicenseStatus.PRO
  }

  private fun nodesUsage(): Int? {
    try {
      val nodes =
        kubernetesClientPermissionHelper
          .map { obj: KubernetesClientPermissionHelper -> obj.listNodes() }
          .orElse(null)

      if (nodes != null) {
        return nodes.list().items.size
      }
    } catch (e: PermissionDeniedException) {
      log.warn { "Permission denied while attempting to get node usage: $e.message" }
    } catch (e: Exception) {
      log.error(e) { "Unexpected error while fetching Kubernetes nodes: $e.message" }
    }
    return null
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
