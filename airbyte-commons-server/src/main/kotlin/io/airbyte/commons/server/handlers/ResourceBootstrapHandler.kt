/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.converters.WorkspaceConverter
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.helpers.buildStandardWorkspace
import io.airbyte.commons.server.handlers.helpers.validateWorkspace
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UnifiedTrial
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Supplier

private val logger = KotlinLogging.logger { }

@Singleton
open class ResourceBootstrapHandler(
  @Named("uuidGenerator") private val uuidSupplier: Supplier<UUID>,
  private val workspaceService: WorkspaceService,
  private val organizationService: OrganizationService,
  private val permissionHandler: PermissionHandler,
  private val currentUserService: CurrentUserService,
  private val roleResolver: RoleResolver,
  private val organizationPaymentConfigService: OrganizationPaymentConfigService,
  private val airbyteEdition: AirbyteEdition,
  private val dataplaneGroupService: DataplaneGroupService,
  private val entitlementService: EntitlementService,
  private val featureFlagClient: FeatureFlagClient,
) : ResourceBootstrapHandlerInterface {
  /**
   * This is for bootstrapping a workspace and all the necessary links (organization) and permissions (workspace & organization).
   */
  override fun bootStrapWorkspaceForCurrentUser(workspaceCreateWithId: WorkspaceCreateWithId): WorkspaceRead {
    val user = currentUserService.getCurrentUser()
    // The organization to use to set up the new workspace
    val organization =
      when (val organizationId = workspaceCreateWithId.organizationId) {
        null -> findOrCreateOrganizationAndPermission(user)
        else ->
          organizationService.getOrganization(organizationId).orElseThrow {
            ConfigNotFoundException(
              ConfigNotFoundType.ORGANIZATION,
              "Attempted to bootstrap workspace but couldn't find existing organization $organizationId",
            )
          }
      }

    // Ensure user has the required permissions to create a workspace
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, organization.organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    val standardWorkspace =
      buildStandardWorkspace(
        workspaceCreateWithId,
        organization,
        uuidSupplier,
        dataplaneGroupService,
      )

    validateWorkspace(standardWorkspace, airbyteEdition)
    workspaceService.writeWorkspaceWithSecrets(standardWorkspace)

    kotlin
      .runCatching {
        permissionHandler.createPermission(
          Permission()
            .withUserId(user.userId)
            .withWorkspaceId(standardWorkspace.workspaceId)
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withPermissionId(uuidSupplier.get()),
        )
      }.onFailure { e ->
        when (e) {
          is PermissionRedundantException ->
            logger.info {
              "Skipped redundant workspace permission creation for workspace ${standardWorkspace.workspaceId}"
            }
          else -> throw e
        }
      }

    return WorkspaceConverter.domainToApiModel(standardWorkspace)
  }

  open fun findOrCreateOrganizationAndPermission(user: AuthenticatedUser): Organization {
    findExistingOrganization(user)?.let { return it }
    val organization =
      Organization().apply {
        this.organizationId = uuidSupplier.get()
        this.userId = user.userId
        this.name = getDefaultOrganizationName(user)
        this.email = user.email
      }

    try {
      // Add the organization to a Stigg trial plan.
      // Note that Stigg is giving users access to features that they might need before they run a sync, so we need to
      // add them to the EntitlementPlan immediately, as opposed to Orb where we wait for a first successful sync.
      if (featureFlagClient.boolVariation(UnifiedTrial, io.airbyte.featureflag.Organization(organization.organizationId))) {
        entitlementService.addOrUpdateOrganization(OrganizationId(organization.organizationId), EntitlementPlan.UNIFIED_TRIAL)
      } else {
        entitlementService.addOrUpdateOrganization(OrganizationId(organization.organizationId), EntitlementPlan.STANDARD_TRIAL)
      }
    } catch (exception: Exception) {
      logger.error(exception) {
        "Failed to add organization ${organization.organizationId} to entitlement service during user signup. "
      }
      // TODO: once we've integrated fully with Stigg, throw instead of just logging
      // throw EntitlementServiceUnableToAddOrganizationProblem(
      //   "Failed to register organization with entitlement service",
      //   ProblemEntitlementServiceData()
      //       .organizationId(organization.organizationId)
      //       .planId(EntitlementPlan.STANDARD_TRIAL.toString())
      //       .errorMessage(exception.message ?: "Unknown entitlement service error")
      // )
    }

    organizationService.writeOrganization(organization)

    organizationPaymentConfigService.saveDefaultPaymentConfig(organization.organizationId)

    permissionHandler.createPermission(
      Permission()
        .withUserId(user.userId)
        .withOrganizationId(organization.organizationId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
        .withPermissionId(uuidSupplier.get()),
    )
    return organization
  }

  /**
   * Tries to find an existing organization for the user. Permission checks will happen elsewhere.
   */
  open fun findExistingOrganization(user: AuthenticatedUser): Organization? {
    val organizationPermissionList = permissionHandler.listPermissionsForUser(user.userId).filter { it.organizationId != null }

    val hasSingleOrganization = organizationPermissionList.size == 1
    val hasNoOrganization = organizationPermissionList.isEmpty()

    val organizationId =
      when {
        hasSingleOrganization -> {
          organizationPermissionList.first().organizationId.let {
            logger.info {
              "User ${user.userId} is associated with only one organization with ID $it"
            }
            it
          }
        }
        hasNoOrganization -> {
          logger.info { "User ${user.userId} is associated with no organization." }
          null
        }
        else -> throw ApplicationErrorKnownException("User is associated with more than one organization. Please specify an organization id.")
      }

    return organizationId?.let { organizationService.getOrganization(it).orElse(null) }
  }

  private fun getDefaultOrganizationName(user: AuthenticatedUser): String =
    when {
      user.companyName != null -> {
        "${user.companyName}'s Organization"
      }

      user.name != null -> {
        "${user.name}'s Organization"
      }

      else -> {
        "${user.email.split("@").first()}'s Organization"
      }
    }
}
