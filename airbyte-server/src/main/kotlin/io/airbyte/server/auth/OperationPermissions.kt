/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.micronaut.context.annotation.Value
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

enum class OperationCaller { USER, INTERNAL_CLIENT, DATAPLANE, SERVICE_ACCOUNT, EMBEDDED, WORKLOAD_API }

@Singleton
class OperationPermissions(
  private val permissions: PermissionService,
  private val roles: RoleResolver,
  @Value("\${micronaut.security.enabled:false}") private val securityEnabled: Boolean,
) {
  fun requireWorkspaces(
    caller: Authentication,
    workspacesAndOrganizations: List<WorkspaceAndOrganization>,
    requiredRoles: Set<String>,
    allowedCallers: Set<OperationCaller>,
  ) {
    if (workspacesAndOrganizations.isEmpty() || requiredRoles.isEmpty()) throw ForbiddenProblem()
    if (!securityEnabled) return
    requireCaller(caller, allowedCallers)
    val scope = getTokenScopeClaim(caller)
    for (workspaceAndOrganization in workspacesAndOrganizations) {
      if (scope != null && scope.workspaceId != workspaceAndOrganization.workspaceId.toString()) throw ForbiddenProblem()
      val available =
        resolve(
          caller,
          setOf(workspaceAndOrganization.workspaceId),
          mapOf(workspaceAndOrganization.workspaceId to workspaceAndOrganization.organizationId),
          setOf(workspaceAndOrganization.organizationId),
        )
      if (requiredRoles.none(available::contains)) throw ForbiddenProblem()
    }
  }

  /** Keep token restrictions while resolving manual current-user checks from the saved human subject. */
  fun currentUser(
    caller: Authentication,
    users: CurrentUserService,
  ): Authentication {
    if (securityEnabled) requireCaller(caller, organizationCallers)
    return Authentication.build(users.getCurrentUser().authUserId, caller.roles, caller.attributes + TokenType.USER.toClaim())
  }

  fun requireSelf(
    caller: Authentication,
    subject: String,
  ) {
    if (!securityEnabled) return
    requireCaller(caller, setOf(OperationCaller.USER))
    if (getTokenScopeClaim(caller) != null) throw ForbiddenProblem()
    if (caller.name != subject) throw ForbiddenProblem()
  }

  /** A content-deduplicated catalog can have several valid saved workspace associations. */
  fun requireAnyWorkspace(
    caller: Authentication,
    candidates: List<WorkspaceAndOrganization>,
    requiredRoles: Set<String>,
    allowedCallers: Set<OperationCaller>,
  ) {
    if (candidates.isEmpty() || requiredRoles.isEmpty()) throw ForbiddenProblem()
    if (!securityEnabled) return
    requireCaller(caller, allowedCallers)
    val scope = getTokenScopeClaim(caller)
    for (candidate in candidates) {
      if (scope != null && scope.workspaceId != candidate.workspaceId.toString()) continue
      val available =
        resolve(caller, setOf(candidate.workspaceId), mapOf(candidate.workspaceId to candidate.organizationId), setOf(candidate.organizationId))
      if (requiredRoles.any(available::contains)) return
    }
    throw ForbiddenProblem()
  }

  fun requireDataplaneIdentity(
    caller: Authentication,
    serviceAccountId: UUID,
  ) {
    if (!securityEnabled) return
    requireCaller(caller, setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT))
    if (getTokenScopeClaim(caller) != null) throw ForbiddenProblem()
    if (TokenType.fromClaims(caller.attributes) != TokenType.INTERNAL_CLIENT && caller.name != serviceAccountId.toString()) throw ForbiddenProblem()
  }

  fun requireOrganizations(
    caller: Authentication,
    organizationIds: List<UUID>,
    requiredRoles: Set<String>,
    allowedCallers: Set<OperationCaller>,
  ) {
    if (organizationIds.isEmpty() || requiredRoles.isEmpty()) throw ForbiddenProblem()
    if (!securityEnabled) return
    requireCaller(caller, allowedCallers)
    if (getTokenScopeClaim(caller) != null) throw ForbiddenProblem()
    for (organizationId in organizationIds) {
      val available = resolve(caller, emptySet(), emptyMap(), setOf(organizationId))
      if (requiredRoles.none(available::contains)) throw ForbiddenProblem()
    }
  }

  fun requireSelfOrInstanceAdmin(
    caller: Authentication,
    subjects: Set<String>,
    allowedCallers: Set<OperationCaller>,
  ) {
    if (subjects.isEmpty() || subjects.any(String::isBlank)) throw ForbiddenProblem()
    if (!securityEnabled) return
    requireCaller(caller, allowedCallers)
    if (getTokenScopeClaim(caller) != null) throw ForbiddenProblem()
    if (TokenType.fromClaims(caller.attributes) == TokenType.USER && caller.name in subjects) return
    if (AuthRoleConstants.ADMIN !in resolve(caller, emptySet(), emptyMap(), emptySet())) throw ForbiddenProblem()
  }

  fun requireGlobal(
    caller: Authentication,
    requiredRoles: Set<String>,
    allowedCallers: Set<OperationCaller>,
  ) {
    if (requiredRoles.isEmpty()) throw ForbiddenProblem()
    if (!securityEnabled) return
    requireCaller(caller, allowedCallers)
    if (getTokenScopeClaim(caller) != null) throw ForbiddenProblem()
    if (requiredRoles.none(resolve(caller, emptySet(), emptyMap(), emptySet())::contains)) throw ForbiddenProblem()
  }

  fun requireAuthenticated(
    caller: Authentication,
    allowedCallers: Set<OperationCaller>,
    owners: List<WorkspaceAndOrganization> = emptyList(),
  ) {
    if (!securityEnabled) return
    requireCaller(caller, allowedCallers)
    val scope = getTokenScopeClaim(caller)
    if (scope != null && (owners.isEmpty() || owners.any { scope.workspaceId != it.workspaceId.toString() })) throw ForbiddenProblem()
  }

  private fun requireCaller(
    caller: Authentication,
    allowed: Set<OperationCaller>,
  ) {
    val kind =
      when (TokenType.fromClaims(caller.attributes)) {
        TokenType.USER -> OperationCaller.USER
        TokenType.INTERNAL_CLIENT -> OperationCaller.INTERNAL_CLIENT
        TokenType.DATAPLANE_V1 -> OperationCaller.DATAPLANE
        TokenType.SERVICE_ACCOUNT -> OperationCaller.SERVICE_ACCOUNT
        TokenType.EMBEDDED_V1 -> OperationCaller.EMBEDDED
        TokenType.WORKLOAD_API -> OperationCaller.WORKLOAD_API
      }
    if (caller.name.isBlank() || kind !in allowed) throw ForbiddenProblem()
  }

  // Reuse the legacy permission calculation and never swallow lookup errors.
  private fun resolve(
    caller: Authentication,
    workspaces: Set<UUID>,
    organizationsByWorkspace: Map<UUID, UUID>,
    organizations: Set<UUID>,
  ): Set<String> {
    val type = TokenType.fromClaims(caller.attributes)
    val permissions =
      when (type) {
        TokenType.INTERNAL_CLIENT -> return AuthRole.getInstanceAdminRoles()
        TokenType.WORKLOAD_API -> return setOf(AuthRoleConstants.DATAPLANE)
        TokenType.EMBEDDED_V1 -> return setOf(AuthRoleConstants.EMBEDDED_END_USER)
        TokenType.USER -> permissions.getPermissionsByAuthUserId(caller.name)
        TokenType.DATAPLANE_V1, TokenType.SERVICE_ACCOUNT -> permissions.getPermissionsByServiceAccountId(UUID.fromString(caller.name))
      }
    return roles.resolveRoles(permissions, caller.name, workspaces, organizationsByWorkspace, organizations, emptySet(), type == TokenType.USER)
  }
}
