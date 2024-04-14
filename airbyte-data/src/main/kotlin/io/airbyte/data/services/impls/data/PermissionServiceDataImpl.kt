package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.config.ConfigSchema
import io.airbyte.config.Permission
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.RemoveLastOrgAdminPermissionException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class PermissionServiceDataImpl(
  private val workspaceService: WorkspaceService,
  private val permissionRepository: PermissionRepository,
) : PermissionService {
  override fun getPermissionsForUser(userId: UUID): List<Permission> {
    return permissionRepository.findByUserId(userId).map { it.toConfigModel() }
  }

  @Transactional("config")
  override fun deletePermission(permissionId: UUID) {
    throwIfDeletingLastOrgAdmin(listOf(permissionId))
    permissionRepository.deleteById(permissionId)
  }

  @Transactional("config")
  override fun deletePermissions(permissionIds: List<UUID>) {
    throwIfDeletingLastOrgAdmin(permissionIds)
    permissionRepository.deleteByIdIn(permissionIds)
  }

  @Transactional("config")
  override fun createPermission(permission: Permission): Permission {
    val existingUserPermissions = getPermissionsForUser(permission.userId).toSet()

    // throw if new permission would be redundant
    if (isRedundantWorkspacePermission(permission, existingUserPermissions)) {
      throw PermissionRedundantException(
        "Permission type ${permission.permissionType} would be redundant for user ${permission.userId}. Preventing creation.",
      )
    }

    // remove any permissions that would be made redundant by adding in the new permission
    deletePermissionsMadeRedundantByPermission(permission, existingUserPermissions)

    return permissionRepository.save(permission.toEntity()).toConfigModel()
  }

  @Transactional("config")
  override fun updatePermission(permission: Permission) {
    // throw early if the update would remove the last org admin
    throwIfUpdateWouldRemoveLastOrgAdmin(permission)

    val otherPermissionsForUser = getPermissionsForUser(permission.userId).filter { it.permissionId != permission.permissionId }.toSet()

    // remove the permission being updated if it is now redundant.
    if (isRedundantWorkspacePermission(permission, otherPermissionsForUser)) {
      permissionRepository.deleteById(permission.permissionId)
      return
    }

    // remove any permissions that would be made redundant by adding in the newly-updated permission
    deletePermissionsMadeRedundantByPermission(permission, otherPermissionsForUser)

    permissionRepository.update(permission.toEntity()).toConfigModel()
  }

  private fun deletePermissionsMadeRedundantByPermission(
    permission: Permission,
    otherPermissions: Set<Permission>,
  ) {
    otherPermissions.filter { isRedundantWorkspacePermission(it, otherPermissions - it + permission) }
      .map { it.permissionId }
      .takeIf { it.isNotEmpty() }
      ?.let { permissionRepository.deleteByIdIn(it) }
  }

  private fun throwIfDeletingLastOrgAdmin(permissionIdsToDelete: List<UUID>) {
    // get all org admin permissions being deleted, if any
    val deletedOrgAdminPermissions =
      permissionRepository.findByIdIn(permissionIdsToDelete).filter {
        it.permissionType == PermissionType.organization_admin
      }

    // group deleted org admin permission IDs by organization ID
    val orgIdToDeletedOrgAdminPermissionIds = deletedOrgAdminPermissions.groupBy({ it.organizationId!! }, { it.id!! })

    // for each group, make sure the last org-admin isn't being deleted
    orgIdToDeletedOrgAdminPermissionIds.forEach {
        (orgId, deletedOrgAdminIds) ->
      throwIfDeletingLastOrgAdminForOrg(orgId, deletedOrgAdminIds.toSet())
    }
  }

  private fun throwIfDeletingLastOrgAdminForOrg(
    orgId: UUID,
    deletedOrgAdminPermissionIds: Set<UUID>,
  ) {
    // get all other permissions for the organization that are not being deleted
    val otherOrgPermissions = permissionRepository.findByOrganizationId(orgId).filter { it.id !in deletedOrgAdminPermissionIds }

    // if there are no other org-admin permissions remaining in the org, throw an exception
    if (otherOrgPermissions.none { it.permissionType == PermissionType.organization_admin }) {
      throw RemoveLastOrgAdminPermissionException("Cannot delete the last admin in Organization $orgId.")
    }
  }

  private fun throwIfUpdateWouldRemoveLastOrgAdmin(updatedPermission: Permission) {
    // return early if the permission is not for an organization
    val orgId = updatedPermission.organizationId ?: return

    // get the current state of the permission in the database
    val priorPermission =
      permissionRepository.findById(updatedPermission.permissionId)
        .orElseThrow { ConfigNotFoundException(ConfigSchema.PERMISSION, "Permission not found: ${updatedPermission.permissionId}") }

    // return early if the permission was not an org admin prior to the update
    if (priorPermission.permissionType != PermissionType.organization_admin) {
      return
    }

    // get all other permissions for the organization
    val otherOrgPermissions = permissionRepository.findByOrganizationId(orgId).filter { it.id != updatedPermission.permissionId }

    // if the permission being updated is the last org admin, throw an exception
    if (otherOrgPermissions.none { it.permissionType == PermissionType.organization_admin }) {
      throw RemoveLastOrgAdminPermissionException("Cannot demote the last admin in Organization $orgId.")
    }
  }

  private fun isRedundantWorkspacePermission(
    permission: Permission,
    existingUserPermissions: Set<Permission>,
  ): Boolean {
    // only workspace permissions can be redundant
    val workspaceId = permission.workspaceId ?: return false

    // if the workspace is not in an organization, it cannot have redundant permissions
    val orgIdForWorkspace = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).orElse(null) ?: return false

    // if the user has no org-level permission, the workspace permission cannot be redundant
    val existingOrgPermission = existingUserPermissions.find { it.organizationId == orgIdForWorkspace } ?: return false

    // if the new permission is less than or equal to the existing org-level permission, it is redundant
    return getAuthority(permission.permissionType) <= getAuthority(existingOrgPermission.permissionType)
  }

  private fun getAuthority(permissionType: Permission.PermissionType): Int {
    return when (permissionType) {
      Permission.PermissionType.INSTANCE_ADMIN -> throw IllegalArgumentException("INSTANCE_ADMIN permissions are not supported")
      Permission.PermissionType.ORGANIZATION_ADMIN -> OrganizationAuthRole.ORGANIZATION_ADMIN.authority
      Permission.PermissionType.ORGANIZATION_EDITOR -> OrganizationAuthRole.ORGANIZATION_EDITOR.authority
      Permission.PermissionType.ORGANIZATION_READER -> OrganizationAuthRole.ORGANIZATION_READER.authority
      Permission.PermissionType.ORGANIZATION_MEMBER -> OrganizationAuthRole.ORGANIZATION_MEMBER.authority
      Permission.PermissionType.WORKSPACE_OWNER -> WorkspaceAuthRole.WORKSPACE_ADMIN.authority
      Permission.PermissionType.WORKSPACE_ADMIN -> WorkspaceAuthRole.WORKSPACE_ADMIN.authority
      Permission.PermissionType.WORKSPACE_EDITOR -> WorkspaceAuthRole.WORKSPACE_EDITOR.authority
      Permission.PermissionType.WORKSPACE_READER -> WorkspaceAuthRole.WORKSPACE_READER.authority
    }
  }
}
