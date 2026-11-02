/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.config.Permission
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.airbyte.publicApi.server.generated.models.GroupMemberAddRequest
import io.airbyte.publicApi.server.generated.models.GroupPermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.PermissionCreateRequest
import io.micronaut.context.annotation.Value
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
class PublicGroupSavedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = GroupOperationInput.Saved(publicResourceId(args.firstOrNull()))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = GroupSavedAdminRule(data, permissions).authorize(caller, input)
}

@Singleton
class PublicGroupMemberAddRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val membership: MembershipAuthorizationDataService,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    GroupOperationInput.Member(
      publicResourceId(args.firstOrNull()),
      (args.getOrNull(1) as? GroupMemberAddRequest ?: throw BadRequestProblem()).userId,
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = GroupMemberAddRule(data, permissions, membership).authorize(caller, input)
}

@Singleton
class PublicGroupPermissionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.getOrNull(1) as? GroupPermissionCreateRequest ?: throw BadRequestProblem()
    return GroupOperationInput.Grant(
      publicResourceId(args.firstOrNull()),
      Optional.ofNullable(body.workspaceId),
      Optional.ofNullable(body.organizationId),
      Permission.PermissionType.valueOf(body.permissionType.name),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = GroupPermissionCreateRule(data, permissions).authorize(caller, input)
}

@Singleton
class PublicGroupPermissionDeleteRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val membership: MembershipAuthorizationDataService,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    GroupOperationInput.PermissionDelete(publicResourceId(args.firstOrNull()), publicResourceId(args.getOrNull(1)))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = GroupPermissionDeleteRule(data, permissions, membership).authorize(caller, input)
}

@Singleton
class PublicGroupListRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.ORGANIZATION, setOf(AuthRoleConstants.ORGANIZATION_ADMIN)) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    if (args.firstOrNull() == null) throw BadRequestProblem(ProblemMessageData().message("organizationId query parameter is required"))
    return super.readInput(args)
  }
}

data class PublicPermissionCreateInput(
  val workspace: Optional<UUID>,
  val organization: Optional<UUID>,
  val user: UUID,
) : OperationInput

@Singleton
class PublicPermissionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.firstOrNull() as? PermissionCreateRequest ?: throw BadRequestProblem()
    return PublicPermissionCreateInput(Optional.ofNullable(body.workspaceId), Optional.ofNullable(body.organizationId), body.userId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val request = input as PublicPermissionCreateInput
    if (request.workspace.isPresent &&
      request.organization.isPresent
    ) {
      throw BadRequestProblem(ProblemMessageData().message("A permission cannot target both a workspace and an organization."))
    }
    val target =
      when {
        request.workspace.isPresent -> AuthorizationTarget.Workspace(data.workspaceAndOrganization(request.workspace.orElseThrow()))
        request.organization.isPresent -> AuthorizationTarget.Organization(data.organization(request.organization.orElseThrow()))
        else -> throw BadRequestProblem(
          ProblemMessageData().message("Workspace ID or Organization ID must be provided in order to create a permission."),
        )
      }
    requireAuthorizationTarget(permissions, caller, target, permissionAdminRoles, callers)
    data.requireUser(request.user)
  }
}

abstract class PublicPermissionSavedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val read: Boolean,
  private val securityEnabled: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = PublicResourceInput(publicResourceId(args.firstOrNull()))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val target = data.permissionTarget((input as PublicResourceInput).id)
    // GLOBAL permission routes retain their INSTANCE_ADMIN restriction even when authentication is disabled.
    if (target == AuthorizationTarget.Global && !securityEnabled) throw ForbiddenProblem()
    val roles =
      if (target ==
        AuthorizationTarget.Global
      ) {
        setOf(AuthRoleConstants.INSTANCE_ADMIN)
      } else if (read) {
        workspaceReadRoles
      } else {
        permissionAdminRoles
      }
    requireAuthorizationTarget(permissions, caller, target, roles, callers)
  }
}

@Singleton
class PublicPermissionReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") securityEnabled: Boolean,
) : PublicPermissionSavedRule(data, permissions, true, securityEnabled)

@Singleton
class PublicPermissionWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") securityEnabled: Boolean,
) : PublicPermissionSavedRule(data, permissions, false, securityEnabled)

data class PublicPermissionListInput(
  val user: Optional<UUID>,
  val organization: Optional<String>,
) : OperationInput

@Singleton
class PublicPermissionListRule(
  private val data: AuthorizationDataService,
  private val membership: MembershipAuthorizationDataService,
  private val permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") private val securityEnabled: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicPermissionListInput(Optional.ofNullable(args.firstOrNull()).map(::publicResourceId), Optional.ofNullable(args.getOrNull(1) as? String))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireAuthenticated(caller, callers)
    val request = input as PublicPermissionListInput
    val identity = if (securityEnabled) membership.currentIdentity(caller.name) else membership.defaultIdentity()
    // SELF returns all the current user's permissions, deliberately ignoring organizationId.
    if (request.user.isEmpty || request.user.orElseThrow() == identity.userId) return
    val organization =
      request.organization.orElseThrow {
        BadRequestProblem(ProblemMessageData().message("Organization ID is required when getting someone else's permissions."))
      }
    permissions.requireOrganizations(
      caller,
      listOf(data.organization(publicResourceId(organization))),
      setOf(AuthRoleConstants.ORGANIZATION_ADMIN),
      callers,
    )
  }
}
