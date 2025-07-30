/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.roles.OrganizationAuthRole
import io.airbyte.commons.auth.roles.WorkspaceAuthRole
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.config.helpers.PermissionHelper
import io.airbyte.data.auth.TokenType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.utils.SecurityService
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
  private val securityService: SecurityService?,
  private val permissionHandler: PermissionHandler,
) {
  data class Subject(
    val id: String,
    val type: TokenType,
  )

  fun newRequest() = Request()

  inner class Request internal constructor() {
    var subject: Subject? = null
    val props: MutableMap<String, String> = mutableMapOf()
    val orgs: MutableSet<UUID> = mutableSetOf()

    fun withCurrentUser() =
      apply {
        subject = Subject(currentUserService.getCurrentUser().authUserId, TokenType.USER)
      }

    fun withSubject(
      id: String,
      type: TokenType,
    ) = apply {
      this.subject = Subject(id, type)
    }

    fun withCurrentAuthentication() =
      apply {
        // In community auth, where micronaut auth is disabled, the security service isn't available,
        // so we need to manually fall back to the default user.
        if (securityService == null) {
          withSubject(DEFAULT_USER_ID.toString(), TokenType.USER)
        }

        securityService?.authentication?.map { auth ->
          logger.debug { "Using current authentication object ${auth.name} ${auth.roles} ${auth.attributes}" }
          withClaims(auth.name, auth.attributes)
        }
      }

    fun withClaims(
      sub: String,
      claims: Map<String, Any>,
    ) = apply {
      // Figure out the subject type and ID.
      subject = Subject(sub, TokenType.fromClaims(claims))
    }

    fun withRefsFromCurrentHttpRequest() =
      apply {
        ServerRequestContext.currentRequest<Any>().map { withRefsFromHttpRequest(it) }
      }

    fun withRefsFromHttpRequest(req: HttpRequest<*>) =
      apply {
        val headers = req.headers.asMap(String::class.java, String::class.java)
        props.putAll(headers)
      }

    fun withOrg(organizationId: UUID) =
      apply {
        orgs.add(organizationId)
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
      // these make null-checking cleaner
      val subject = subject

      logger.debug { "Resolving roles for $subject" }

      if (subject == null) {
        logger.debug { "subject is null, returning empty role set" }
        return emptySet()
      }
      if (subject.id.isBlank()) {
        logger.debug { "subject.id is blank, returning empty role set" }
        return emptySet()
      }

      return try {
        when (subject.type) {
          // Certain token types have a hard-coded list of roles.
          TokenType.WORKLOAD_API -> setOf(AuthRoleConstants.DATAPLANE)
          TokenType.EMBEDDED_V1 -> setOf(AuthRoleConstants.EMBEDDED_END_USER)
          TokenType.LEGACY_KEYCLOAK_SERVICE_ACCOUNT -> AuthRole.getInstanceAdminRoles()
          // Everything else resolves roles via the "permissions" table.
          TokenType.DATAPLANE_V1, TokenType.SERVICE_ACCOUNT ->
            resolvePermissions(
              subject.id,
              permissionHandler.getPermissionsByServiceAccountId(UUID.fromString(subject.id)),
            )
          TokenType.USER -> resolvePermissions(subject.id, permissionHandler.getPermissionsByAuthUserId(subject.id))
          TokenType.INTERNAL_CLIENT -> AuthRole.getInstanceAdminRoles()
        }
      } catch (e: Exception) {
        logger.error(e) { "Failed to resolve roles for $subject" }
        return emptySet()
      }
    }

    private fun resolvePermissions(
      subjectId: String,
      perms: List<Permission>,
    ): Set<String> {
      logger.debug { "Resolving permissions for $subject and $perms" }

      val workspaceIds = authenticationHeaderResolver.resolveWorkspace(props)?.toSet() ?: emptySet()
      val resolvedOrgIds = authenticationHeaderResolver.resolveOrganization(props)?.toSet() ?: emptySet()
      val authUserIds = authenticationHeaderResolver.resolveAuthUserIds(props.toMap()) ?: emptySet()
      val allOrgIds = orgs + resolvedOrgIds

      return resolveRoles(perms, subjectId, workspaceIds, allOrgIds, authUserIds)
    }

    /**
     * requireRole checks whether the request has the given role available,
     * and if not it throws ForbiddenProblem.
     */
    fun requireRole(role: String) {
      if (!roles().contains(role)) {
        throw ForbiddenProblem(
          ProblemMessageData().message("Caller does not have the required $role permissions to access the resource(s)."),
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
    subjectId: String,
    workspaceIds: Set<UUID>,
    organizationIds: Set<UUID>,
    authUserIds: Set<String>,
  ): Set<String> {
    logger.debug {
      "Resolving roles for $subjectId with perms=$perms workspaceIds=$workspaceIds organizationIds=$organizationIds authUserIds=$authUserIds"
    }

    val roles = mutableSetOf(AuthRoleConstants.AUTHENTICATED_USER)

    // The SELF role denotes that request refers to the current request's identity.
    //
    // This relies on the assumption that the AuthenticationHeaderResolver will only
    // ever resolve authUserIds for one user (who can have multiple authUserIds).
    // TODO Technically, that's a weak assumption and we should make that interface clearer.
    if (authUserIds.contains(subjectId)) {
      roles.add(AuthRoleConstants.SELF)
    }

    if (perms.any { it.permissionType == PermissionType.INSTANCE_ADMIN }) {
      roles.addAll(AuthRole.getInstanceAdminRoles())
    }

    perms.filter { it.permissionType == PermissionType.DATAPLANE }.forEach {
      if (it.workspaceId == null && it.organizationId == null) {
        roles.add(AuthRoleConstants.DATAPLANE)
      }
    }

    determineWorkspaceRole(perms, workspaceIds)?.let {
      roles.addAll(impliedRoles(it))
    }
    determineOrganizationRole(perms, organizationIds)?.let {
      roles.addAll(impliedRoles(it))
    }

    logger.debug {
      "Resolved roles for $subjectId with perms=$perms workspaceIds=$workspaceIds organizationIds=$organizationIds authUserIds=$authUserIds to $roles"
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
//
// perms == the full list of the permissions a user has
// workspaceIds == the set of workspace IDs that the request is referring to.
private fun determineWorkspaceRole(
  perms: List<Permission>,
  workspaceIds: Set<UUID>,
): PermissionType? {
  if (workspaceIds.isEmpty()) return null
  if (perms.isEmpty()) return null

  // Filters out organization level permissions
  val workspacePerms = perms.filter { it.workspaceId != null }
  val permWorkspaceIds = workspacePerms.map { it.workspaceId }.toSet()
  // There must be a permission for every workspace. If not, then return null.
  if (!permWorkspaceIds.containsAll(workspaceIds)) {
    return null
  }
  return workspacePerms
    // Make sure we're only getting the min permission from permissions tied to the requested workspace Ids
    .filter { it.workspaceId in workspaceIds }
    .minByOrNull {
      it.permissionType
        .convertTo<WorkspaceAuthRole>()
        ?.getAuthority()
        ?: Integer.MAX_VALUE
    }?.permissionType
}

// Determine the minimal role that the permissions grant to the given organizations.
//
// See determineWorkspaceRole() for more details about "minimal".
//
// perms == the full list of the permissions a user has
// organizationIds == the set of workspace IDs that the request is referring to.
private fun determineOrganizationRole(
  perms: List<Permission>,
  organizationIds: Set<UUID>,
): PermissionType? {
  if (organizationIds.isEmpty()) return null
  if (perms.isEmpty()) return null

  // Filters out workspace level permissions
  val orgPerms = perms.filter { it.organizationId != null }
  val permOrgIds = orgPerms.map { it.organizationId }.toSet()
  // There must be a permission for every org. If not, then return null.
  if (!permOrgIds.containsAll(organizationIds)) {
    return null
  }
  return orgPerms
    // Make sure we're only getting the min permission from permissions tied to the requested organization Ids
    .filter { it.organizationId in organizationIds }
    .minByOrNull {
      it.permissionType
        .convertTo<OrganizationAuthRole>()
        ?.getAuthority()
        ?: Integer.MAX_VALUE
    }?.permissionType
}
