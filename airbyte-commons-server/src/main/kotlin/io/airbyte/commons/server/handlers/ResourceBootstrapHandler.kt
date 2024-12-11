package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.converters.WorkspaceConverter
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.helpers.buildStandardWorkspace
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigSchema
import io.airbyte.config.Organization
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Supplier

val DEFAULT_WORKSPACE_PERMISSION_TYPE = PermissionType.WORKSPACE_ADMIN
val DEFAULT_ORGANIZATION_PERMISSION_TYPE = PermissionType.ORGANIZATION_ADMIN

val logger = KotlinLogging.logger { }

@Singleton
open class ResourceBootstrapHandler(
  @Named("uuidGenerator") private val uuidSupplier: Supplier<UUID>,
  private val workspaceService: WorkspaceService,
  private val organizationService: OrganizationService,
  private val permissionService: PermissionService,
  private val currentUserService: CurrentUserService,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val featureFlagClient: FeatureFlagClient,
  private val organizationPaymentConfigService: OrganizationPaymentConfigService,
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
              ConfigSchema.ORGANIZATION,
              "Attempted to bootstrap workspace but couldn't find existing organization $organizationId",
            )
          }
      }

    // Ensure user has the required permissions to create a workspace
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      Scope.ORGANIZATION,
      listOf(organization.organizationId.toString()),
      setOf(OrganizationAuthRole.ORGANIZATION_ADMIN),
    )

    val standardWorkspace = buildStandardWorkspace(workspaceCreateWithId, organization, uuidSupplier)
    workspaceService.writeWorkspaceWithSecrets(standardWorkspace)

    val workspacePermission = buildDefaultWorkspacePermission(user.userId, standardWorkspace.workspaceId)

    kotlin.runCatching { permissionService.createPermission(workspacePermission) }.onFailure { e ->
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

  fun findOrCreateOrganizationAndPermission(user: AuthenticatedUser): Organization {
    findExistingOrganization(user)?.let { return it }
    val organization =
      Organization().apply {
        this.organizationId = uuidSupplier.get()
        this.userId = user.userId
        this.name = getDefaultOrganizationName(user)
        this.email = user.email
      }
    organizationService.writeOrganization(organization)

    val paymentConfig =
      OrganizationPaymentConfig()
        .withOrganizationId(organization.organizationId)
        .withPaymentStatus(OrganizationPaymentConfig.PaymentStatus.UNINITIALIZED)
        .withSubscriptionStatus(OrganizationPaymentConfig.SubscriptionStatus.PRE_SUBSCRIPTION)

    organizationPaymentConfigService.savePaymentConfig(paymentConfig)

    val organizationPermission = buildDefaultOrganizationPermission(user.userId, organization.organizationId)
    permissionService.createPermission(organizationPermission)
    return organization
  }

  /**
   * Tries to find an existing organization for the user. Permission checks will happen elsewhere.
   */
  open fun findExistingOrganization(user: AuthenticatedUser): Organization? {
    val organizationPermissionList = permissionService.getPermissionsForUser(user.userId).filter { it.organizationId != null }

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

  private fun buildDefaultWorkspacePermission(
    userId: UUID,
    workspaceId: UUID,
  ): Permission =
    Permission().apply {
      this.userId = userId
      this.workspaceId = workspaceId
      this.permissionType = DEFAULT_WORKSPACE_PERMISSION_TYPE
      this.permissionId = uuidSupplier.get()
    }

  private fun buildDefaultOrganizationPermission(
    userId: UUID,
    organizationId: UUID,
  ): Permission =
    Permission().apply {
      this.userId = userId
      this.organizationId = organizationId
      this.permissionType = DEFAULT_ORGANIZATION_PERMISSION_TYPE
      this.permissionId = uuidSupplier.get()
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
