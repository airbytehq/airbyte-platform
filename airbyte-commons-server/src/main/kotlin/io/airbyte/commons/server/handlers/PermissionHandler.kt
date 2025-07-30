/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionCheckRequest
import io.airbyte.api.model.generated.PermissionDeleteUserFromWorkspaceRequestBody
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionReadList
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.PermissionUpdate
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.lang.Exceptions
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.config.Permission
import io.airbyte.config.UserPermission
import io.airbyte.config.helpers.PermissionHelper.definedPermissionGrantsTargetPermission
import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.RemoveLastOrgAdminPermissionException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.io.IOException
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * PermissionHandler, provides basic CRUD operation access for permissions.
 */
@Singleton
open class PermissionHandler(
  private val permissionPersistence: PermissionPersistence?,
  private val workspaceService: WorkspaceService,
  @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>?,
  private val permissionService: PermissionService,
) {
  /**
   * Creates a new permission.
   *
   * @param permissionCreate The new permission.
   * @return The created permission.
   * @throws IOException if unable to retrieve the existing permissions.
   * @throws JsonValidationException if unable to validate the existing permission data.
   */
  @Throws(IOException::class, JsonValidationException::class, PermissionRedundantException::class)
  fun createPermission(permissionCreate: Permission): Permission {
    // INSTANCE_ADMIN permissions are only created in special cases, so we block them here.

    if (permissionCreate.permissionType == Permission.PermissionType.INSTANCE_ADMIN) {
      throw JsonValidationException("Cannot create INSTANCE_ADMIN permission record.")
    }

    if (permissionCreate.permissionType == Permission.PermissionType.DATAPLANE) {
      throw JsonValidationException("Cannot create DATAPLANE_ADMIN permission record.")
    }

    // Look for an existing permission.
    val existingPermissions = permissionService.getPermissionsForUser(permissionCreate.userId)
    for (p in existingPermissions) {
      if (checkPermissionsAreEqual(permissionCreate, p)) {
        return p
      }
    }

    if (permissionCreate.permissionId == null) {
      permissionCreate.permissionId = uuidGenerator?.get()
    }

    return permissionService.createPermission(permissionCreate)
  }

  @Throws(PermissionRedundantException::class)
  fun grantInstanceAdmin(userId: UUID?) {
    permissionService.createPermission(
      Permission()
        .withPermissionId(uuidGenerator?.get())
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN),
    )
  }

  @Throws(io.airbyte.config.persistence.ConfigNotFoundException::class, IOException::class)
  fun getPermissionById(permissionId: UUID): Permission = permissionService.getPermission(permissionId)

  private fun checkPermissionsAreEqual(
    permission: Permission,
    permissionCreate: Permission,
  ): Boolean {
    if (permission.permissionType != permissionCreate.permissionType) {
      return false
    }
    if (permission.workspaceId == null && permissionCreate.workspaceId != null) {
      return false
    }
    if (permission.workspaceId != null && permission.workspaceId != permissionCreate.workspaceId) {
      return false
    }
    if (permission.organizationId == null && permissionCreate.organizationId != null) {
      return false
    }
    if (permission.organizationId != null && permission.organizationId != permissionCreate.organizationId) {
      return false
    }
    return true
  }

  /**
   * Gets a permission by permission Id.
   *
   * @param permissionIdRequestBody request body including permission id.
   * @return The queried permission.
   * @throws IOException if unable to get the permissions.
   * @throws ConfigNotFoundException if unable to get the permissions.
   * @throws JsonValidationException if unable to get the permissions.
   */
  @Throws(io.airbyte.config.persistence.ConfigNotFoundException::class, IOException::class)
  fun getPermissionRead(permissionIdRequestBody: PermissionIdRequestBody): PermissionRead {
    val permission = getPermissionById(permissionIdRequestBody.permissionId)
    return buildPermissionRead(permission)
  }

  /**
   * Updates the permissions.
   *
   *
   * We only allow updating permission type between workspace level roles OR organization level roles.
   *
   *
   * Valid examples: 1. update "workspace_xxx" to "workspace_yyy" 2. update "organization_xxx" to
   * "organization_yyy" (only invalid when demoting the LAST organization_admin role in an org to
   * another organization level role)
   *
   *
   * Invalid examples: 1. update "instance_admin" to any other types 2. update "workspace_xxx" to
   * "organization_xxx"/"instance_admin" 3. update "organization_xxx" to
   * "workspace_xxx"/"instance_admin"
   *
   * @param permissionUpdate The permission update.
   * @throws IOException if unable to update the permissions.
   * @throws ConfigNotFoundException if unable to update the permissions.
   * @throws OperationNotAllowedException if update is prevented by business logic.
   */
  @Throws(
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    OperationNotAllowedException::class,
    JsonValidationException::class,
  )
  fun updatePermission(permissionUpdate: PermissionUpdate) {
    // INSTANCE_ADMIN permissions are only created in special cases, so we block them here.

    if (permissionUpdate.permissionType == PermissionType.INSTANCE_ADMIN) {
      throw JsonValidationException("Cannot update permission record to INSTANCE_ADMIN.")
    }

    val existingPermission = getPermissionById(permissionUpdate.permissionId)

    val updatedPermission =
      Permission()
        .withPermissionId(permissionUpdate.permissionId)
        .withPermissionType(permissionUpdate.permissionType.convertTo<Permission.PermissionType>())
        .withOrganizationId(existingPermission.organizationId) // cannot be updated
        .withWorkspaceId(existingPermission.workspaceId) // cannot be updated
        .withUserId(existingPermission.userId) // cannot be updated
    try {
      permissionService.updatePermission(updatedPermission)
    } catch (e: RemoveLastOrgAdminPermissionException) {
      throw ConflictException(e.message, e)
    }
  }

  /**
   * Checks the permissions associated with a user. All user permissions are fetched and each one is
   * checked against the requested permission. If any of the user's permissions meet the requirements
   * of the permission check, then the check succeeds.
   *
   * @param permissionCheckRequest The permission check request.
   * @return The result of the permission check.
   * @throws IOException if unable to check the permission.
   */
  @Throws(IOException::class)
  fun checkPermissions(permissionCheckRequest: PermissionCheckRequest): PermissionCheckRead {
    val userPermissions = permissionReadListForUser(permissionCheckRequest.userId).permissions

    val anyMatch =
      userPermissions.stream().anyMatch { userPermission: PermissionRead ->
        Exceptions.toRuntime<Boolean> {
          checkPermissions(
            permissionCheckRequest,
            userPermission,
          )
        }
      }

    return PermissionCheckRead().status(if (anyMatch) PermissionCheckRead.StatusEnum.SUCCEEDED else PermissionCheckRead.StatusEnum.FAILED)
  }

  /**
   * Checks whether a particular user permission meets the requirements of a particular permission
   * check request. Organization-level user permissions grant workspace-level permissions as long as
   * the workspace in question belongs to the user's organization, so this method contains logic to
   * see if the requested permission is for a workspace that the user permission should grant access
   * to.
   */
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.config.persistence.ConfigNotFoundException::class)
  private fun checkPermissions(
    permissionCheckRequest: PermissionCheckRequest,
    userPermission: PermissionRead,
  ): Boolean {
    if (mismatchedUserIds(userPermission, permissionCheckRequest)) {
      return false
    }

    // if the user is an instance admin, return true immediately, since instance admins have access to
    // everything by definition.
    if (userPermission.permissionType == PermissionType.INSTANCE_ADMIN) {
      return true
    }

    if (mismatchedWorkspaceIds(userPermission, permissionCheckRequest)) {
      return false
    }

    if (mismatchedOrganizationIds(userPermission, permissionCheckRequest)) {
      return false
    }

    if (requestedWorkspaceNotInOrganization(userPermission, permissionCheckRequest)) {
      return false
    }

    // by this point, we know we can directly compare the user permission's type to the requested
    // permission's type, because all underlying user/workspace/organization IDs are valid.
    return definedPermissionGrantsTargetPermission(
      userPermission.permissionType.convertTo<Permission.PermissionType>(),
      permissionCheckRequest.permissionType.convertTo<Permission.PermissionType>(),
    )
  }

  // check if this permission request is for a user that doesn't match the user permission.
  // in practice, this shouldn't happen because we fetch user permissions based on the request.
  private fun mismatchedUserIds(
    userPermission: PermissionRead,
    request: PermissionCheckRequest,
  ): Boolean = userPermission.userId != request.userId

  // check if this permission request is for a workspace that doesn't match the user permission.
  private fun mismatchedWorkspaceIds(
    userPermission: PermissionRead,
    request: PermissionCheckRequest,
  ): Boolean = userPermission.workspaceId != null && userPermission.workspaceId != request.workspaceId

  // check if this permission request is for an organization that doesn't match the user permission.
  private fun mismatchedOrganizationIds(
    userPermission: PermissionRead,
    request: PermissionCheckRequest,
  ): Boolean = userPermission.organizationId != null && request.organizationId != null && (userPermission.organizationId != request.organizationId)

  // check if this permission request is for a workspace that belongs to a different organization than
  // the user permission.
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.config.persistence.ConfigNotFoundException::class)
  private fun requestedWorkspaceNotInOrganization(
    userPermission: PermissionRead,
    request: PermissionCheckRequest,
  ): Boolean {
    // if the user permission is for an organization, and the request is for a workspace, return true if
    // the workspace
    // does not belong to the organization.

    if (userPermission.organizationId != null && request.workspaceId != null) {
      val requestedWorkspaceOrganizationId: UUID?
      try {
        requestedWorkspaceOrganizationId = workspaceService.getStandardWorkspaceNoSecrets(request.workspaceId, false).organizationId
      } catch (e: ConfigNotFoundException) {
        throw io.airbyte.config.persistence
          .ConfigNotFoundException(e.type, e.configId)
      }
      // If the workspace is not in any organization, return true
      if (requestedWorkspaceOrganizationId == null) {
        return true
      }
      return requestedWorkspaceOrganizationId != userPermission.organizationId
    }

    // else, not a workspace-level request with an org-level user permission, so return false.
    return false
  }

  /**
   * Given multiple workspaceIds, checks whether the user has at least the given permissionType for
   * all workspaceIds.
   *
   * @param multiRequest The permissions check request with multiple workspaces
   * @return The result of the permission check.
   * @throws IOException If unable to check the permission.
   */
  fun permissionsCheckMultipleWorkspaces(multiRequest: PermissionsCheckMultipleWorkspacesRequest): PermissionCheckRead {
    // Turn the multiple-request into a list of individual requests, one per workspace

    val permissionCheckRequests =
      multiRequest.workspaceIds
        .stream()
        .map { workspaceId: UUID? ->
          PermissionCheckRequest()
            .userId(multiRequest.userId)
            .permissionType(multiRequest.permissionType)
            .workspaceId(workspaceId)
        }.toList()

    // Perform the individual permission checks and store the results in a list
    val results =
      permissionCheckRequests
        .stream()
        .map { permissionCheckRequest: PermissionCheckRequest ->
          try {
            return@map checkPermissions(permissionCheckRequest)
          } catch (e: IOException) {
            log.error(e) { "Error checking permissions for request: $permissionCheckRequest" }
            return@map PermissionCheckRead().status(PermissionCheckRead.StatusEnum.FAILED)
          }
        }.toList()

    // If each individual workspace check succeeded, return an overall success. Otherwise, return an
    // overall failure.
    return if (results.stream().allMatch { result: PermissionCheckRead -> result.status == PermissionCheckRead.StatusEnum.SUCCEEDED }) {
      PermissionCheckRead().status(PermissionCheckRead.StatusEnum.SUCCEEDED)
    } else {
      PermissionCheckRead().status(PermissionCheckRead.StatusEnum.FAILED)
    }
  }

  fun isUserInstanceAdmin(userId: UUID): Boolean =
    permissionService
      .getPermissionsForUser(userId)
      .stream()
      .anyMatch { it: Permission -> it.permissionType == Permission.PermissionType.INSTANCE_ADMIN }

  /**
   * Lists the permissions by user.
   *
   * @param userId The user ID.
   * @return The permissions for the given user.
   * @throws IOException if unable to retrieve the permissions for the user.
   * @throws JsonValidationException if unable to retrieve the permissions for the user.
   */
  @Throws(IOException::class)
  fun permissionReadListForUser(userId: UUID): PermissionReadList {
    val permissions = permissionService.getPermissionsForUser(userId)
    return PermissionReadList().permissions(
      permissions
        .stream()
        .map { permission: Permission -> buildPermissionRead(permission) }
        .collect(Collectors.toList<@Valid PermissionRead?>()),
    )
  }

  @Throws(IOException::class)
  fun listPermissionsForUser(userId: UUID): List<Permission> = permissionService.getPermissionsForUser(userId)

  /**
   * Lists the permissions by user in an organization.
   *
   * @param userId The user ID.
   * @param organizationId The organization ID.
   * @return The permissions for the given user in the given organization.
   * @throws IOException if unable to retrieve the permissions for the user.
   * @throws JsonValidationException if unable to retrieve the permissions for the user.
   */
  @Throws(IOException::class)
  fun listPermissionsByUserInAnOrganization(
    userId: UUID,
    organizationId: UUID,
  ): PermissionReadList {
    val permissions =
      permissionService
        .getPermissionsForUser(userId)
        .stream()
        .filter { it: Permission -> organizationId == it.organizationId }
        .toList()
    return PermissionReadList().permissions(
      permissions
        .stream()
        .map { permission: Permission -> buildPermissionRead(permission) }
        .collect(Collectors.toList<@Valid PermissionRead?>()),
    )
  }

  /**
   * Deletes a permission.
   *
   * @param permissionIdRequestBody The permission to be deleted.
   * @throws ConflictException if deletion is prevented by business logic.
   */
  fun deletePermission(permissionIdRequestBody: PermissionIdRequestBody) {
    try {
      permissionService.deletePermission(permissionIdRequestBody.permissionId)
    } catch (e: RemoveLastOrgAdminPermissionException) {
      throw ConflictException(e.message, e)
    }
  }

  /**
   * Delete all permission records that match a particular userId and workspaceId.
   */
  @Throws(IOException::class)
  fun deleteUserFromWorkspace(deleteUserFromWorkspaceRequestBody: PermissionDeleteUserFromWorkspaceRequestBody) {
    val userId = deleteUserFromWorkspaceRequestBody.userId
    val workspaceId = deleteUserFromWorkspaceRequestBody.workspaceId

    // delete all workspace-level permissions that match the userId and workspaceId
    val userWorkspacePermissionIds =
      permissionService
        .getPermissionsForUser(userId)
        .stream()
        .filter { permission: Permission -> permission.workspaceId != null && permission.workspaceId == workspaceId }
        .map { obj: Permission -> obj.permissionId }
        .toList()

    try {
      permissionService.deletePermissions(userWorkspacePermissionIds)
    } catch (e: RemoveLastOrgAdminPermissionException) {
      throw ConflictException(e.message, e)
    }
  }

  fun getPermissionsByAuthUserId(authUserId: String): List<Permission> = permissionService.getPermissionsByAuthUserId(authUserId)

  fun getPermissionsByServiceAccountId(serviceAccountId: UUID): List<Permission> =
    permissionService.getPermissionsByServiceAccountId(serviceAccountId)

  @Throws(IOException::class)
  fun listUsersInOrganization(organizationId: UUID): List<UserPermission> =
    permissionPersistence?.listUsersInOrganization(organizationId) ?: emptyList()

  @Throws(IOException::class)
  fun listInstanceAdminUsers(): List<UserPermission> = permissionPersistence?.listInstanceAdminUsers() ?: emptyList()

  @Throws(IOException::class)
  fun listPermissionsForOrganization(organizationId: UUID): List<UserPermission> =
    permissionPersistence?.listPermissionsForOrganization(organizationId) ?: emptyList()

  fun countInstanceEditors(): Int {
    val editorRoles =
      setOf(
        Permission.PermissionType.ORGANIZATION_EDITOR,
        Permission.PermissionType.ORGANIZATION_ADMIN,
        Permission.PermissionType.ORGANIZATION_RUNNER,
        Permission.PermissionType.WORKSPACE_EDITOR,
        Permission.PermissionType.WORKSPACE_OWNER,
        Permission.PermissionType.WORKSPACE_ADMIN,
        Permission.PermissionType.WORKSPACE_RUNNER,
      )

    return permissionService
      .listPermissions()
      .stream()
      .filter { p: Permission -> editorRoles.contains(p.permissionType) }
      .map { obj: Permission -> obj.userId }
      .collect(Collectors.toSet())
      .size
  }

  @Throws(IOException::class)
  fun findPermissionTypeForUserAndOrganization(
    organizationId: UUID,
    authUserId: String,
  ): Permission.PermissionType? = permissionPersistence?.findPermissionTypeForUserAndOrganization(organizationId, authUserId)

  companion object {
    private val log = KotlinLogging.logger {}

    private fun buildPermissionRead(permission: Permission): PermissionRead =
      PermissionRead()
        .permissionId(permission.permissionId)
        .userId(permission.userId)
        .permissionType(
          permission.permissionType.convertTo<PermissionType>(),
        ).workspaceId(permission.workspaceId)
        .organizationId(permission.organizationId)
  }
}
