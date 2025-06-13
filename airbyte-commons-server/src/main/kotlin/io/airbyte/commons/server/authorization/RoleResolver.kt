/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.config.helpers.PermissionHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * The RoleResolver helps determine the roles available for a request.
 *
 * Given an identity (e.g. user) and set of IDs referenced by the request
 * e.g. (workspace, organization, source, destination, etc.)
 * RoleResolver returns a set of roles matching those parameters.
 *
 * RoleResolver will return all the roles up to the granted permission.
 * For example, if the user is granted WORKSPACE_EDITOR, then RoleResolver will return
 * `setOf(WORKSPACE_EDITOR, WORKSPACE_RUNNER, WORKSPACE_READER)`.
 *
 * RoleResolver expects that the user is always authenticated,
 * so the AUTHENTICATED_USER is _always_ returned (unless there's an error).
 *
 * RoleResolver.Request is a builder-style object that helps build up the details
 * relevant to a request.
 *
 * For example, the following verifies that the current user can access a source ID
 * references in the request:
 * ```
 *   roleResolver.Request()
 *     // use CurrentUserService to determine the current user
 *     .withCurrentUser()
 *     // the request references a source ID
 *     .withRef(AuthenticationId.SOURCE_ID, sourceId)
 *     // require the workspace-reader role, or throw an error
 *     .requireRole(AuthRoleConstants.WORKSPACE_READER)
 * ```
 */
@Singleton
open class RoleResolver(
  private val authenticationHeaderResolver: AuthenticationHeaderResolver,
  private val currentUserService: CurrentUserService,
  private val permissionHandler: PermissionHandler,
) {
  inner class Request {
    var authUserId: String? = null
    val props: MutableMap<String, String> = mutableMapOf()

    fun withCurrentUser() =
      apply {
        authUserId = currentUserService.currentUser.authUserId
      }

    fun withAuthUserId(authUserId: String) =
      apply {
        this.authUserId = authUserId
      }

    fun withCurrentHttpRequest() =
      apply {
        ServerRequestContext.currentRequest<Any>().map { withHttpRequest(it) }
      }

    fun withHttpRequest(req: HttpRequest<*>) =
      apply {
        val headers = req.headers.asMap(String::class.java, String::class.java)
        props.putAll(headers)
      }

    fun withRef(
      key: AuthenticationId,
      value: String,
    ) = apply {
      props[key.httpHeader] = value
    }

    fun withRef(
      key: AuthenticationId,
      value: UUID,
    ) = apply {
      withRef(key, value.toString())
    }

    fun withWorkspaces(ids: List<UUID>) =
      apply {
        withRef(AuthenticationId.WORKSPACE_IDS, Jsons.serialize(ids))
      }

    /**
     * roles() resolves the request details into a set of available roles.
     */
    fun roles(): Set<String> {
      logger.debug { "Resolving roles for authUserId $authUserId" }

      try {
        val user = authUserId
        if (user.isNullOrBlank()) {
          logger.debug { "Provided authUserId is null or blank, returning empty role set" }
          return emptySet()
        }

        val workspaceIds = authenticationHeaderResolver.resolveWorkspace(props)?.toSet() ?: emptySet()
        val organizationIds = authenticationHeaderResolver.resolveOrganization(props)?.toSet() ?: emptySet()
        val authUserIds = authenticationHeaderResolver.resolveAuthUserIds(props) ?: emptySet()
        val perms = permissionHandler.getPermissionsByAuthUserId(authUserId)
        return resolveRoles(perms, user, workspaceIds, organizationIds, authUserIds)
      } catch (e: Exception) {
        logger.error(e) { "Failed to resolve roles for authUserId $authUserId" }
        return emptySet()
      }
    }

    /**
     * requireRole checks whether the request has the given role available,
     * and if not it throws ForbiddenProblem.
     */
    fun requireRole(role: String) {
      if (!roles().contains(role)) {
        throw ForbiddenProblem(
          ProblemMessageData().message("User does not have the required $role permissions to access the resource(s)."),
        )
      }
    }
  }

  /**
   * resolveRoles is the heart of RoleResolver. Given details about the request,
   * resolveRoles() returns the set of roles available for the current request.
   *
   * @param perms - the list of permissions the current user has.
   * @param currentAuthUserId - the authUserId of the current user.
   * @param workspaceIds - list of workspace IDs the user is requesting access to.
   * @param organizationIds - list of organization IDs the user is requesting access to.
   * @param authUserIds - list of authUserIds the user is requesting access to.
   */
  fun resolveRoles(
    perms: List<Permission>,
    currentAuthUserId: String,
    workspaceIds: Set<UUID>,
    organizationIds: Set<UUID>,
    authUserIds: Set<String>,
  ): Set<String> {
    val roles = mutableSetOf(AuthRoleConstants.AUTHENTICATED_USER)

    // The SELF role denotes that request refers to the current request's identity.
    //
    // This relies on the assumption that the AuthenticationHeaderResolver will only
    // ever resolve authUserIds for one user (who can have multiple authUserIds).
    // TODO Technically, that's a weak assumption and we should make that interface clearer.
    if (authUserIds.contains(currentAuthUserId)) {
      roles.add(AuthRoleConstants.SELF)
    }

    if (perms.any { it.permissionType == PermissionType.INSTANCE_ADMIN }) {
      roles.addAll(AuthRole.getInstanceAdminRoles())
    }

    determineWorkspaceRole(perms, workspaceIds)?.let {
      roles.addAll(impliedRoles(it))
    }
    determineOrganizationRole(perms, organizationIds)?.let {
      roles.addAll(impliedRoles(it))
    }

    return roles
  }
}

private fun impliedRoles(perm: PermissionType): List<String> = PermissionHelper.getGrantedPermissions(perm).map { it.name }

// Determine the minimal role that the permissions grant to the given workspaces.
//
// "minimal" means, for example, that if the permissions grant "admin" on workspace 1,
// but only "reader" on workspaces 2 and 3, then this function returns only "reader",
// because that's the minimal role the permissions grant across _all_ the workspaces.
//
// Similarly, if the permissions grant no access to a given workspace,
// then this function returns an NONE (no access).
private fun determineWorkspaceRole(
  perms: List<Permission>,
  workspaceIds: Set<UUID>,
): PermissionType? {
  if (workspaceIds.isEmpty()) return null
  if (perms.isEmpty()) return null

  val workspacePerms = perms.filter { it.workspaceId != null }
  val permOrgIds = workspacePerms.map { it.workspaceId }.toSet()
  // There must be a permission for every workspace. If not, then return null.
  if (!permOrgIds.containsAll(workspaceIds)) {
    return null
  }
  return workspacePerms
    .minByOrNull {
      Enums
        .convertTo(it.permissionType, WorkspaceAuthRole::class.java)
        ?.authority
        ?: Integer.MAX_VALUE
    }?.permissionType
}

// Determine the minimal role that the permissions grant to the given organizations.
//
// See determineWorkspaceRole() for more details about "minimal".
private fun determineOrganizationRole(
  perms: List<Permission>,
  organizationIds: Set<UUID>,
): PermissionType? {
  if (organizationIds.isEmpty()) return null
  if (perms.isEmpty()) return null

  val orgPerms = perms.filter { it.organizationId != null }
  val permOrgIds = orgPerms.map { it.organizationId }.toSet()
  // There must be a permission for every org. If not, then return null.
  if (!permOrgIds.containsAll(organizationIds)) {
    return null
  }
  return orgPerms
    .minByOrNull {
      Enums
        .convertTo(it.permissionType, OrganizationAuthRole::class.java)
        ?.authority
        ?: Integer.MAX_VALUE
    }?.permissionType
}
