/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.RemoveLastOrgAdminPermissionException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.mockk.Runs
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class PermissionDaoDataImplTest {
  private val testUserId = UUID.randomUUID()
  private val testServiceAccountId = UUID.randomUUID()

  private lateinit var workspaceService: WorkspaceService
  private lateinit var permissionRepository: PermissionRepository
  private lateinit var permissionService: PermissionDaoDataImpl

  @BeforeEach
  fun setUp() {
    workspaceService = mockk()
    permissionRepository = mockk()
    permissionService = PermissionDaoDataImpl(workspaceService, permissionRepository)
  }

  @Nested
  inner class GetPermissionsForUser {
    @Test
    fun `getPermissionsForUser should fetch from repository`() {
      val permissions =
        listOf(
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            workspaceId = UUID.randomUUID()
            permissionType = PermissionType.WORKSPACE_ADMIN
          },
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = UUID.randomUUID()
            permissionType = PermissionType.ORGANIZATION_ADMIN
          },
        )

      every { permissionRepository.findByUserId(testUserId) } returns permissions.map { it.toEntity() }

      val result = permissionService.getPermissionsForUser(testUserId)

      assertEquals(result.toSet(), permissions.toSet())

      verify { permissionRepository.findByUserId(testUserId) }
      confirmVerified(permissionRepository)
    }

    @Test
    fun `getPermissionsByServiceAccountId returns permissions based on a service account`() {
      val permissions =
        listOf(
          Permission().apply {
            permissionId = UUID.randomUUID()
            serviceAccountId = testServiceAccountId
            workspaceId = UUID.randomUUID()
            permissionType = PermissionType.DATAPLANE
          },
        )

      every { permissionRepository.findByServiceAccountId(testServiceAccountId) } returns permissions.map { it.toEntity() }

      val result = permissionService.getPermissionsByServiceAccountId(testServiceAccountId)

      assertEquals(result.toSet(), permissions.toSet())

      verify { permissionRepository.findByServiceAccountId(eq(testServiceAccountId)) }
      confirmVerified(permissionRepository)
    }
  }

  @Nested
  inner class DeletePermission {
    @Test
    fun `deletePermission should delete from repository if not the last org-admin`() {
      val permId = UUID.randomUUID()
      val orgId = UUID.randomUUID()

      val permissionToDelete =
        Permission().apply {
          permissionId = permId
          userId = testUserId
          organizationId = orgId
          permissionType = PermissionType.ORGANIZATION_ADMIN
        }

      every { permissionRepository.findByIdIn(listOf(permId)) } returns listOf(permissionToDelete.toEntity())

      every { permissionRepository.findByOrganizationId(orgId) } returns
        listOf(
          permissionToDelete,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_ADMIN // another org admin exists for a different user
          },
        ).map { it.toEntity() }

      every { permissionRepository.deleteByIdIn(any()) } just Runs
      every { permissionRepository.findByUserId(testUserId) } returns listOf(permissionToDelete.toEntity())

      permissionService.deletePermission(permId)

      verify { permissionRepository.findByIdIn(listOf(permId)) }
      verify { permissionRepository.findByUserId(testUserId) }
      verify { permissionRepository.findByOrganizationId(orgId) }
      verify { permissionRepository.deleteByIdIn(listOf(permId)) }
      confirmVerified(permissionRepository)
    }

    @Test
    fun `deletePermission should throw when deleting last org admin`() {
      val permId = UUID.randomUUID()
      val orgId = UUID.randomUUID()

      val permissionToDelete =
        Permission().apply {
          permissionId = permId
          userId = testUserId
          organizationId = orgId
          permissionType = PermissionType.ORGANIZATION_ADMIN
        }

      every { permissionRepository.findByIdIn(listOf(permId)) } returns listOf(permissionToDelete.toEntity())

      every { permissionRepository.findByOrganizationId(orgId) } returns
        listOf(
          permissionToDelete,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_EDITOR // only other perm in the org is editor, so throw
          },
        ).map { it.toEntity() }

      assertThrows<RemoveLastOrgAdminPermissionException> { permissionService.deletePermission(permId) }

      verify { permissionRepository.findByIdIn(listOf(permId)) }
      verify { permissionRepository.findByOrganizationId(orgId) }
      verify(exactly = 0) { permissionRepository.deleteByIdIn(any()) }
      confirmVerified(permissionRepository)
    }

    @Test
    fun `deletePermission for org permission should cascade to workspace permissions`() {
      val permId = UUID.randomUUID()
      val orgId = UUID.randomUUID()
      val workspaceInOrgId = UUID.randomUUID()
      val workspaceOutOfOrgId = UUID.randomUUID()
      val workspacePermissionInOrg =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          workspaceId = workspaceInOrgId
          permissionType = PermissionType.WORKSPACE_ADMIN
        }

      val workspacePermissionOutOfOrg =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          workspaceId = workspaceOutOfOrgId
          permissionType = PermissionType.WORKSPACE_ADMIN
        }

      val permissionToDelete =
        Permission().apply {
          permissionId = permId
          userId = testUserId
          organizationId = orgId
          permissionType = PermissionType.ORGANIZATION_ADMIN
        }

      every { permissionRepository.findByIdIn(listOf(permId)) } returns listOf(permissionToDelete.toEntity())

      every { permissionRepository.findByOrganizationId(orgId) } returns
        listOf(
          permissionToDelete,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_ADMIN
          },
        ).map { it.toEntity() }

      every { permissionRepository.findByUserId(testUserId) } returns
        listOf(
          permissionToDelete,
          workspacePermissionInOrg,
          workspacePermissionOutOfOrg,
        ).map { it.toEntity() }

      val workspaceInOrg =
        StandardWorkspace().apply {
          workspaceId = workspaceInOrgId
          name = "workspaceInOrg"
          organizationId = orgId
          email = "email@email.com"
        }

      val workspaceOutOfOrg =
        StandardWorkspace().apply {
          workspaceId = workspaceOutOfOrgId
          name = "workspaceOutOfOrg"
          organizationId = UUID.randomUUID()
          email = "email@email.com"
        }

      every {
        workspaceService.listStandardWorkspacesWithIds(listOf(workspaceInOrgId, workspaceOutOfOrgId), true)
      } returns listOf(workspaceInOrg, workspaceOutOfOrg)

      every { permissionRepository.deleteByIdIn(any()) } just Runs
      every { permissionRepository.deleteById(any()) } just Runs

      permissionService.deletePermission(permId)

      verify { permissionRepository.findByIdIn(listOf(permId)) }
      verify { permissionRepository.findByUserId(testUserId) }
      verify { permissionRepository.findByOrganizationId(orgId) }
      verify(exactly = 1) { permissionRepository.deleteByIdIn(listOf(permissionToDelete.permissionId, workspacePermissionInOrg.permissionId)) }
      confirmVerified(permissionRepository)
    }
  }

  @Nested
  inner class DeletePermissions {
    @Test
    fun `deletePermissions should delete from repository when not deleting the last org admin`() {
      val permId1 = UUID.randomUUID()
      val permId2 = UUID.randomUUID()
      val orgId = UUID.randomUUID()

      val permissionToDelete1 =
        Permission().apply {
          permissionId = permId1
          userId = testUserId
          organizationId = orgId
          permissionType = PermissionType.ORGANIZATION_ADMIN
        }

      val permissionToDelete2 =
        Permission().apply {
          permissionId = permId2
          userId = testUserId
          organizationId = orgId
          permissionType = PermissionType.ORGANIZATION_EDITOR
        }

      every { permissionRepository.findByIdIn(listOf(permId1, permId2)) } returns
        listOf(
          permissionToDelete1.toEntity(),
          permissionToDelete2.toEntity(),
        )

      every { permissionRepository.findByOrganizationId(orgId) } returns
        listOf(
          permissionToDelete1,
          permissionToDelete2,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_ADMIN // another org admin exists for a different user, so don't throw
          },
        ).map { it.toEntity() }

      every { permissionRepository.findByUserId(testUserId) } returns
        listOf(
          permissionToDelete1,
          permissionToDelete2,
        ).map { it.toEntity() }

      every { permissionRepository.deleteByIdIn(listOf(permId1, permId2)) } just Runs

      permissionService.deletePermissions(listOf(permId1, permId2))

      verify { permissionRepository.findByIdIn(listOf(permId1, permId2)) }
      verify(exactly = 1) { permissionRepository.findByUserId(testUserId) }
      verify { permissionRepository.findByOrganizationId(orgId) }
      verify { permissionRepository.deleteByIdIn(listOf(permId1, permId2)) }
      confirmVerified(permissionRepository)
    }

    @Test
    fun `deletePermissions should throw when deleting the last org admin`() {
      val permId1 = UUID.randomUUID()
      val permId2 = UUID.randomUUID()
      val orgId1 = UUID.randomUUID()
      val orgId2 = UUID.randomUUID()

      val permissionToDelete1 =
        Permission().apply {
          permissionId = permId1
          userId = testUserId
          organizationId = orgId1
          permissionType = PermissionType.ORGANIZATION_ADMIN // not the last admin in org 1
        }

      val permissionToDelete2 =
        Permission().apply {
          permissionId = permId2
          userId = testUserId
          organizationId = orgId2
          permissionType = PermissionType.ORGANIZATION_ADMIN // is the last admin in org 2, should throw
        }

      every { permissionRepository.findByIdIn(listOf(permId1, permId2)) } returns
        listOf(
          permissionToDelete1.toEntity(),
          permissionToDelete2.toEntity(),
        )

      every { permissionRepository.findByOrganizationId(orgId1) } returns
        listOf(
          permissionToDelete1,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId1
            permissionType = PermissionType.ORGANIZATION_ADMIN // another admin exists in org 1, so this doesn't cause the throw
          },
        ).map { it.toEntity() }

      every { permissionRepository.findByOrganizationId(orgId2) } returns
        listOf(
          permissionToDelete2,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId2
            permissionType = PermissionType.ORGANIZATION_EDITOR // only other perm in org 2 is editor, so this causes a throw
          },
        ).map { it.toEntity() }

      assertThrows<RemoveLastOrgAdminPermissionException> { permissionService.deletePermissions(listOf(permId1, permId2)) }

      verify { permissionRepository.findByIdIn(listOf(permId1, permId2)) }
      verify { permissionRepository.findByOrganizationId(orgId1) }
      verify { permissionRepository.findByOrganizationId(orgId2) }
      verify(exactly = 0) { permissionRepository.deleteByIdIn(any()) }
      confirmVerified(permissionRepository)
    }

    @Test
    fun `deletePermissions for org permission should cascade to workspace permissions`() {
      val permId1 = UUID.randomUUID()
      val permId2 = UUID.randomUUID()
      val orgId1 = UUID.randomUUID()
      val orgId2 = UUID.randomUUID()
      val workspaceInOrgId = UUID.randomUUID()
      val workspaceOutOfOrgId = UUID.randomUUID()

      val workspacePermissionInOrg =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          workspaceId = workspaceInOrgId
          permissionType = PermissionType.WORKSPACE_ADMIN
        }

      val workspacePermissionOutOfOrg =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          workspaceId = workspaceOutOfOrgId
          permissionType = PermissionType.WORKSPACE_ADMIN
        }

      val permissionToDelete1 =
        Permission().apply {
          permissionId = permId1
          userId = testUserId
          organizationId = orgId1
          permissionType = PermissionType.ORGANIZATION_ADMIN // not the last admin in org 1
        }

      val permissionToDelete2 =
        Permission().apply {
          permissionId = permId2
          userId = testUserId
          organizationId = orgId2
          permissionType = PermissionType.ORGANIZATION_ADMIN // is the last admin in org 2, should throw
        }

      every { permissionRepository.findByIdIn(listOf(permId1, permId2)) } returns
        listOf(
          permissionToDelete1.toEntity(),
          permissionToDelete2.toEntity(),
        )

      every { permissionRepository.findByOrganizationId(orgId1) } returns
        listOf(
          permissionToDelete1,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId1
            permissionType = PermissionType.ORGANIZATION_ADMIN // another admin exists in org 1, so this doesn't cause the throw
          },
          workspacePermissionInOrg,
          workspacePermissionOutOfOrg,
        ).map { it.toEntity() }

      every { permissionRepository.findByOrganizationId(orgId2) } returns
        listOf(
          permissionToDelete2,
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = UUID.randomUUID()
            organizationId = orgId1
            permissionType = PermissionType.ORGANIZATION_ADMIN // another admin exists in org 1, so this doesn't cause the throw
          },
        ).map { it.toEntity() }

      every { permissionRepository.findByUserId(testUserId) } returns
        listOf(
          permissionToDelete1,
          permissionToDelete2,
          workspacePermissionInOrg,
          workspacePermissionOutOfOrg,
        ).map { it.toEntity() }

      val workspaceInOrg =
        StandardWorkspace().apply {
          workspaceId = workspaceInOrgId
          name = "workspaceInOrg"
          organizationId = orgId1
          email = "email@email.com"
        }

      // This workspace is out of both orgs.
      val workspaceOutOfOrg =
        StandardWorkspace().apply {
          workspaceId = workspaceOutOfOrgId
          name = "workspaceOutOfOrg"
          organizationId = UUID.randomUUID()
          email = "email@email.com"
        }

      every {
        workspaceService.listStandardWorkspacesWithIds(listOf(workspaceInOrgId, workspaceOutOfOrgId), true)
      } returns listOf(workspaceInOrg, workspaceOutOfOrg)
      every { permissionRepository.deleteByIdIn(any()) } just Runs

      permissionService.deletePermissions(listOf(permId1, permId2))

      verify { permissionRepository.findByIdIn(listOf(permId1, permId2)) }
      verify { permissionRepository.findByUserId(testUserId) }
      verify { permissionRepository.findByOrganizationId(orgId1) }
      verify { permissionRepository.findByOrganizationId(orgId2) }
      verify(exactly = 1) {
        permissionRepository.deleteByIdIn(
          listOf(
            permissionToDelete1.permissionId,
            permissionToDelete2.permissionId,
            workspacePermissionInOrg.permissionId,
          ),
        )
      }
      confirmVerified(permissionRepository)
    }
  }

  @Nested
  inner class CreatePermission {
    @Test
    fun `createPermission should save permission when no redundant permissions exist`() {
      val existingOrgPermission =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          organizationId = UUID.randomUUID()
          permissionType = PermissionType.ORGANIZATION_EDITOR
        }
      val existingPermissionDifferentOrg =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          organizationId = UUID.randomUUID()
          permissionType = PermissionType.ORGANIZATION_ADMIN // different org than new permission
        }
      val newPermission =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          workspaceId = UUID.randomUUID()
          permissionType = PermissionType.WORKSPACE_ADMIN // higher than existing org permission, not redundant
        }

      every { permissionRepository.findByUserId(testUserId) } returns
        listOf(
          existingOrgPermission.toEntity(),
          existingPermissionDifferentOrg.toEntity(),
        )
      every { workspaceService.getOrganizationIdFromWorkspaceId(newPermission.workspaceId) } returns
        Optional.of(
          existingOrgPermission.organizationId,
        )
      every { permissionRepository.save(newPermission.toEntity()) } returns newPermission.toEntity()

      val result = permissionService.createPermission(newPermission)

      assertEquals(result, newPermission)

      verify { permissionRepository.findByUserId(testUserId) }
      verify(exactly = 1) { permissionRepository.save(newPermission.toEntity()) }
      confirmVerified(permissionRepository)
    }

    @Test
    fun `createPermission should throw when redundant permission is detected`() {
      val existingOrgPermission =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          organizationId = UUID.randomUUID()
          permissionType = PermissionType.ORGANIZATION_ADMIN
        }
      val newPermission =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          workspaceId = UUID.randomUUID()
          permissionType = PermissionType.WORKSPACE_ADMIN // equal to existing org permission, redundant
        }

      // new permission is for a workspace that belongs to the existing permission's org
      every { workspaceService.getOrganizationIdFromWorkspaceId(newPermission.workspaceId) } returns
        Optional.of(
          existingOrgPermission.organizationId,
        )
      every { permissionRepository.findByUserId(testUserId) } returns listOf(existingOrgPermission.toEntity())

      assertThrows<PermissionRedundantException> { permissionService.createPermission(newPermission) }

      // nothing saved or deleted
      verify(exactly = 0) { permissionRepository.save(any()) }
      verify(exactly = 0) { permissionRepository.deleteById(any()) }
    }

    @Test
    fun `createPermission should work for instance admin permissions`() {
      val newPermission =
        Permission().apply {
          permissionId = UUID.randomUUID()
          userId = testUserId
          permissionType = PermissionType.INSTANCE_ADMIN
        }

      every { permissionRepository.findByUserId(testUserId) } returns emptyList()
      every { permissionRepository.save(newPermission.toEntity()) } returns newPermission.toEntity()

      val result = permissionService.createPermission(newPermission)

      assertEquals(result, newPermission)

      verify { permissionRepository.findByUserId(testUserId) }
      verify(exactly = 1) { permissionRepository.save(newPermission.toEntity()) }
      confirmVerified(permissionRepository)
    }
  }

  @Nested
  inner class UpdatePermission {
    @Nested
    inner class UpdateWorkspacePermission {
      @Test
      fun `updatePermission should update workspace permission when not redundant`() {
        val existingOrgPermission =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = UUID.randomUUID()
            permissionType = PermissionType.ORGANIZATION_READER // lower than updated permission, so nothing redundant
          }
        val existingPermissionDifferentOrg =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = UUID.randomUUID()
            permissionType = PermissionType.ORGANIZATION_ADMIN // different org than new permission, so nothing redundant
          }
        val workspacePermissionPreUpdate =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            workspaceId = UUID.randomUUID()
            permissionType = PermissionType.WORKSPACE_ADMIN
          }
        val updatedWorkspacePermission =
          Permission().apply {
            permissionId = workspacePermissionPreUpdate.permissionId
            userId = workspacePermissionPreUpdate.userId
            workspaceId = workspacePermissionPreUpdate.workspaceId
            permissionType = PermissionType.WORKSPACE_EDITOR // update from admin to editor
          }

        every { permissionRepository.findByUserId(testUserId) } returns
          listOf(
            existingOrgPermission.toEntity(),
            existingPermissionDifferentOrg.toEntity(),
            workspacePermissionPreUpdate.toEntity(),
          )
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspacePermissionPreUpdate.workspaceId) } returns
          Optional.of(
            existingOrgPermission.organizationId,
          )
        every { permissionRepository.update(updatedWorkspacePermission.toEntity()) } returns updatedWorkspacePermission.toEntity()

        permissionService.updatePermission(updatedWorkspacePermission)

        verify { permissionRepository.findByUserId(testUserId) }
        verify(exactly = 1) { permissionRepository.update(updatedWorkspacePermission.toEntity()) }
        confirmVerified(permissionRepository)
      }

      @Test
      fun `updatePermission should delete updated workspace permission when made redundant`() {
        val existingOrgPermission =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = UUID.randomUUID()
            permissionType = PermissionType.ORGANIZATION_EDITOR // higher than updated permission, so update becomes redundant
          }
        val workspacePermissionPreUpdate =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            workspaceId = UUID.randomUUID()
            permissionType = PermissionType.WORKSPACE_ADMIN
          }
        val workspacePermissionUpdated =
          Permission().apply {
            permissionId = workspacePermissionPreUpdate.permissionId
            userId = workspacePermissionPreUpdate.userId
            workspaceId = workspacePermissionPreUpdate.workspaceId
            permissionType = PermissionType.WORKSPACE_READER // update from admin to reader, permission is now redundant
          }

        every { permissionRepository.findByUserId(testUserId) } returns
          listOf(
            existingOrgPermission.toEntity(),
            workspacePermissionPreUpdate.toEntity(),
          )
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspacePermissionPreUpdate.workspaceId) } returns
          Optional.of(
            existingOrgPermission.organizationId,
          )
        every { permissionRepository.update(workspacePermissionUpdated.toEntity()) } returns workspacePermissionUpdated.toEntity()
        every { permissionRepository.deleteById(workspacePermissionUpdated.permissionId) } just Runs

        permissionService.updatePermission(workspacePermissionUpdated)

        verify { permissionRepository.findByUserId(testUserId) }
        verify(exactly = 0) { permissionRepository.update(any()) } // no update because deleted instead
        verify(exactly = 1) { permissionRepository.deleteById(workspacePermissionUpdated.permissionId) }
        confirmVerified(permissionRepository)
      }
    }

    @Nested
    inner class UpdateOrgPermission {
      @Test
      fun `updatePermission should delete any workspace permissions that are made redundant by updating an org permission`() {
        val existingWorkspacePermission =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            workspaceId = UUID.randomUUID()
            permissionType = PermissionType.WORKSPACE_ADMIN // will be made redundant by updated org permission
          }
        val orgPermissionPreUpdate =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = UUID.randomUUID()
            permissionType = PermissionType.ORGANIZATION_READER
          }
        val updatedOrgPermission =
          Permission().apply {
            permissionId = orgPermissionPreUpdate.permissionId
            userId = orgPermissionPreUpdate.userId
            organizationId = orgPermissionPreUpdate.organizationId
            permissionType = PermissionType.ORGANIZATION_ADMIN // update from org reader to admin
          }

        every { permissionRepository.findById(orgPermissionPreUpdate.permissionId) } returns Optional.of(orgPermissionPreUpdate.toEntity())
        every { permissionRepository.findByUserId(testUserId) } returns
          listOf(
            existingWorkspacePermission.toEntity(),
            orgPermissionPreUpdate.toEntity(),
          )
        every { workspaceService.getOrganizationIdFromWorkspaceId(existingWorkspacePermission.workspaceId) } returns
          Optional.of(
            orgPermissionPreUpdate.organizationId,
          )
        every { permissionRepository.update(updatedOrgPermission.toEntity()) } returns updatedOrgPermission.toEntity()
        every { permissionRepository.deleteByIdIn(listOf(existingWorkspacePermission.permissionId)) } just Runs

        permissionService.updatePermission(updatedOrgPermission)

        verify { permissionRepository.findById(orgPermissionPreUpdate.permissionId) }
        verify { permissionRepository.findByUserId(testUserId) }
        verify(exactly = 1) { permissionRepository.update(updatedOrgPermission.toEntity()) }
        verify(exactly = 1) { permissionRepository.deleteByIdIn(listOf(existingWorkspacePermission.permissionId)) }
        confirmVerified(permissionRepository)
      }

      @Test
      fun `updatePermission should throw if demoting the last org admin`() {
        val orgId = UUID.randomUUID()

        val existingOtherOrgPermission =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_EDITOR // other org permission is not admin
          }
        val orgPermissionPreUpdate =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_ADMIN
          }
        val orgPermissionUpdated =
          Permission().apply {
            permissionId = orgPermissionPreUpdate.permissionId
            userId = orgPermissionPreUpdate.userId
            organizationId = orgPermissionPreUpdate.organizationId
            permissionType =
              PermissionType.ORGANIZATION_EDITOR // org permission update is from admin to editor, throws because it's the last admin
          }

        every { permissionRepository.findById(orgPermissionPreUpdate.permissionId) } returns Optional.of(orgPermissionPreUpdate.toEntity())
        every { permissionRepository.findByOrganizationId(orgId) } returns
          listOf(
            existingOtherOrgPermission.toEntity(),
            orgPermissionPreUpdate.toEntity(),
          )

        assertThrows<RemoveLastOrgAdminPermissionException> { permissionService.updatePermission(orgPermissionUpdated) }

        verify { permissionRepository.findById(orgPermissionPreUpdate.permissionId) }
        verify { permissionRepository.findByOrganizationId(orgId) }
        verify(exactly = 0) { permissionRepository.update(any()) }
        confirmVerified(permissionRepository)
      }

      @Test
      fun `updatePermission should allow org admin demotion if another org admin exists`() {
        val orgId = UUID.randomUUID()

        val existingOtherOrgPermission =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_ADMIN // other org permission is admin
          }
        val orgPermissionPreUpdate =
          Permission().apply {
            permissionId = UUID.randomUUID()
            userId = testUserId
            organizationId = orgId
            permissionType = PermissionType.ORGANIZATION_ADMIN
          }
        val orgPermissionUpdated =
          Permission().apply {
            permissionId = orgPermissionPreUpdate.permissionId
            userId = orgPermissionPreUpdate.userId
            organizationId = orgPermissionPreUpdate.organizationId
            permissionType = PermissionType.ORGANIZATION_EDITOR // org permission update is from admin to editor
          }

        every { permissionRepository.findByUserId(testUserId) } returns
          listOf(
            existingOtherOrgPermission.toEntity(),
            orgPermissionPreUpdate.toEntity(),
          )
        every { permissionRepository.findById(orgPermissionPreUpdate.permissionId) } returns
          Optional.of(
            orgPermissionPreUpdate.toEntity(),
          )
        every { permissionRepository.findByOrganizationId(orgId) } returns
          listOf(
            existingOtherOrgPermission.toEntity(),
            orgPermissionPreUpdate.toEntity(),
          )
        every { permissionRepository.update(orgPermissionUpdated.toEntity()) } returns orgPermissionUpdated.toEntity()

        permissionService.updatePermission(orgPermissionUpdated)

        verify { permissionRepository.findByUserId(testUserId) }
        verify { permissionRepository.findById(orgPermissionPreUpdate.permissionId) }
        verify { permissionRepository.findByOrganizationId(orgId) }
        verify(exactly = 1) { permissionRepository.update(orgPermissionUpdated.toEntity()) }
        confirmVerified(permissionRepository)
      }
    }
  }
}
