/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.PermissionCheckRead
import io.airbyte.api.server.generated.models.PermissionCheckRequest
import io.airbyte.api.server.generated.models.PermissionDeleteUserFromWorkspaceRequestBody
import io.airbyte.api.server.generated.models.PermissionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.PermissionUpdate
import io.airbyte.api.server.generated.models.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.RemoveLastOrgAdminPermissionException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.UUID
import java.util.function.Supplier

internal class PermissionHandlerTest {
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var permissionPersistence: PermissionPersistence
  private lateinit var workspaceService: WorkspaceService
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var permissionService: PermissionService

  @BeforeEach
  fun setUp() {
    permissionPersistence = mock<PermissionPersistence>()
    uuidSupplier = mock<Supplier<UUID>>()
    workspaceService = mock<WorkspaceService>()
    permissionService = mock<PermissionService>()
    permissionHandler = PermissionHandler(permissionPersistence, workspaceService, uuidSupplier, permissionService)
  }

  @Nested
  internal inner class CreatePermission {
    private val userId: UUID = UUID.randomUUID()
    private val workspaceId: UUID = UUID.randomUUID()
    private val permissionId: UUID = UUID.randomUUID()
    private val permission: Permission =
      Permission()
        .withPermissionId(permissionId)
        .withUserId(userId)
        .withWorkspaceId(workspaceId)
        .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

    @Test
    fun testCreatePermission() {
      val existingPermissions = mutableListOf<Permission>()
      whenever(permissionService.getPermissionsForUser(anyOrNull()))
        .thenReturn(existingPermissions)
      whenever(uuidSupplier.get()).thenReturn(permissionId)
      val permissionCreate =
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_OWNER)
          .withUserId(userId)
          .withWorkspaceId(workspaceId)
      whenever(permissionService.createPermission(anyOrNull())).thenReturn(permission)
      val actual = permissionHandler.createPermission(permissionCreate)
      val expected =
        Permission()
          .withPermissionId(permissionId)
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withUserId(userId)
          .withWorkspaceId(workspaceId)

      Assertions.assertEquals(expected, actual)
    }

    @Test
    fun testCreateInstanceAdminPermissionThrows() {
      val permissionCreate =
        Permission()
          .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
          .withUserId(userId)
      Assertions.assertThrows(
        JsonValidationException::class.java,
      ) { permissionHandler.createPermission(permissionCreate) }
    }

    @Test
    fun `permission with both workspace and organization scope is rejected before delegation`() {
      val permissionCreate =
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withUserId(userId)
          .withWorkspaceId(workspaceId)
          .withOrganizationId(UUID.randomUUID())

      Assertions.assertThrows(JsonValidationException::class.java) {
        permissionHandler.createPermission(permissionCreate)
      }

      verify(permissionService, times(0)).createPermission(anyOrNull())
    }

    @Test
    fun `inactive SCIM User grant becomes an HTTP conflict`() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(emptyList())
      whenever(permissionService.createPermission(anyOrNull()))
        .thenThrow(InactiveUserAccessException("inactive"))

      Assertions.assertThrows(ConflictException::class.java) {
        permissionHandler.createPermission(permission)
      }
    }
  }

  @Nested
  internal inner class UpdatePermission {
    private val organizationId: UUID = UUID.randomUUID()

    private val user: AuthenticatedUser =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withName("User")
        .withEmail("user@email.com")

    private val permissionWorkspaceReader: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(user.userId)
        .withWorkspaceId(UUID.randomUUID())
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)

    private val permissionOrganizationAdmin: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(user.userId)
        .withOrganizationId(organizationId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

    @BeforeEach
    fun setup() {
      whenever(permissionService.getPermission(permissionWorkspaceReader.permissionId))
        .thenReturn(
          Permission()
            .withPermissionId(permissionWorkspaceReader.permissionId)
            .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
            .withWorkspaceId(permissionWorkspaceReader.workspaceId)
            .withUserId(permissionWorkspaceReader.userId),
        )

      whenever(permissionService.getPermission(permissionOrganizationAdmin.permissionId))
        .thenReturn(
          Permission()
            .withPermissionId(permissionOrganizationAdmin.permissionId)
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(permissionOrganizationAdmin.organizationId)
            .withUserId(permissionOrganizationAdmin.userId),
        )
    }

    @Test
    fun updatesPermission() {
      val update =
        PermissionUpdate(
          permissionId = permissionWorkspaceReader.permissionId,
          permissionType = PermissionType.WORKSPACE_ADMIN,
        ) // changing to workspace_admin

      permissionHandler.updatePermission(update)

      verify(permissionService).updatePermission(
        Permission()
          .withPermissionId(permissionWorkspaceReader.permissionId)
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withUserId(permissionWorkspaceReader.userId)
          .withWorkspaceId(permissionWorkspaceReader.workspaceId)
          .withOrganizationId(null),
      )
    }

    @Test
    fun testUpdateToInstanceAdminPermissionThrows() {
      val permissionUpdate =
        PermissionUpdate(
          permissionType = PermissionType.INSTANCE_ADMIN,
          permissionId = permissionOrganizationAdmin.permissionId,
        )
      Assertions.assertThrows(
        JsonValidationException::class.java,
      ) { permissionHandler.updatePermission(permissionUpdate) }
    }

    @Test
    fun throwsConflictExceptionIfServiceBlocksUpdate() {
      val update =
        PermissionUpdate(
          permissionId = permissionOrganizationAdmin.permissionId,
          permissionType = PermissionType.ORGANIZATION_EDITOR,
        ) // changing to organization_editor

      doAnswer { throw RemoveLastOrgAdminPermissionException("test") }
        .whenever(permissionService)
        .updatePermission(anyOrNull())
      Assertions.assertThrows(ConflictException::class.java) { permissionHandler.updatePermission(update) }
    }

    @Test
    fun throwsWhenPermissionTypeIsMissing() {
      val update = PermissionUpdate(permissionId = permissionWorkspaceReader.permissionId, permissionType = null)

      Assertions.assertThrows(NullPointerException::class.java) { permissionHandler.updatePermission(update) }

      verify(permissionService, times(0)).updatePermission(anyOrNull())
    }

    @Test
    fun workspacePermissionUpdatesDoNotModifyIdFields() {
      val workspacePermissionUpdate =
        PermissionUpdate(
          permissionId = permissionWorkspaceReader.permissionId,
          permissionType = PermissionType.WORKSPACE_EDITOR,
        ) // changing to workspace_editor

      permissionHandler.updatePermission(workspacePermissionUpdate)

      verify(permissionService).updatePermission(
        Permission()
          .withPermissionId(permissionWorkspaceReader.permissionId)
          .withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
          .withWorkspaceId(permissionWorkspaceReader.workspaceId) // workspace ID preserved from original permission
          .withUserId(permissionWorkspaceReader.userId),
      ) // user ID preserved from original permission
    }

    @Test
    fun organizationPermissionUpdatesDoNotModifyIdFields() {
      val orgPermissionUpdate =
        PermissionUpdate(
          permissionId = permissionOrganizationAdmin.permissionId,
          permissionType = PermissionType.ORGANIZATION_EDITOR,
        ) // changing to organization_editor

      permissionHandler.updatePermission(orgPermissionUpdate)

      verify(permissionService).updatePermission(
        Permission()
          .withPermissionId(permissionOrganizationAdmin.permissionId)
          .withPermissionType(Permission.PermissionType.ORGANIZATION_EDITOR)
          .withOrganizationId(permissionOrganizationAdmin.organizationId) // organization ID preserved from original permission
          .withUserId(permissionOrganizationAdmin.userId),
      ) // user ID preserved from original permission
    }
  }

  @Nested
  internal inner class DeletePermission {
    private val organizationId: UUID = UUID.randomUUID()

    private val user: AuthenticatedUser =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withName("User")
        .withEmail("user@email.com")

    private val permissionWorkspaceReader: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(user.userId)
        .withWorkspaceId(UUID.randomUUID())
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)

    private val permissionOrganizationAdmin: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(user.userId)
        .withOrganizationId(organizationId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

    @Test
    fun deletesPermission() {
      whenever(permissionService.getPermission(permissionWorkspaceReader.permissionId)).thenReturn(permissionWorkspaceReader)

      permissionHandler.deletePermission(PermissionIdRequestBody(permissionId = permissionWorkspaceReader.permissionId))

      verify(permissionService).deletePermission(permissionWorkspaceReader.permissionId)
    }

    @Test
    fun throwsConflictIfPersistenceBlocks() {
      whenever(permissionService.getPermission(permissionOrganizationAdmin.permissionId)).thenReturn(permissionOrganizationAdmin)
      doAnswer { throw RemoveLastOrgAdminPermissionException("test") }
        .whenever(permissionService)
        .deletePermission(anyOrNull())

      Assertions.assertThrows(ConflictException::class.java) {
        permissionHandler.deletePermission(
          PermissionIdRequestBody(permissionId = permissionOrganizationAdmin.permissionId),
        )
      }
    }
  }

  @Nested
  internal inner class CheckPermissions {
    private val workspaceId: UUID = UUID.randomUUID()
    private val organizationId: UUID = UUID.randomUUID()
    private val userId: UUID = UUID.randomUUID()

    @Test
    fun mismatchedUserId() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withUserId(userId),
        ),
      )

      val request =
        PermissionCheckRequest(
          permissionType = PermissionType.WORKSPACE_ADMIN,
          userId = UUID.randomUUID(),
          workspaceId = workspaceId,
        )

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.Status.FAILED, result.status)
    }

    @Test
    fun mismatchedWorkspaceId() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(workspaceId)))

      val request =
        PermissionCheckRequest(
          permissionType = PermissionType.WORKSPACE_ADMIN,
          userId = userId,
          workspaceId = UUID.randomUUID(),
        ) // different workspace

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.Status.FAILED, result.status)
    }

    @Test
    fun mismatchedOrganizationId() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
        ),
      )

      val request =
        PermissionCheckRequest(
          permissionType = PermissionType.ORGANIZATION_ADMIN,
          userId = userId,
          organizationId = UUID.randomUUID(),
        ) // different organization

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.Status.FAILED, result.status)
    }

    @Test
    fun permissionsCheckMultipleWorkspaces() {
      val otherWorkspaceId = UUID.randomUUID()
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withUserId(userId)
            .withWorkspaceId(workspaceId),
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
            .withUserId(userId)
            .withWorkspaceId(otherWorkspaceId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId, otherWorkspaceId), false))
        .thenReturn(
          listOf(
            StandardWorkspace().withWorkspaceId(workspaceId),
            StandardWorkspace().withWorkspaceId(otherWorkspaceId),
          ),
        )

      // EDITOR fails because READER is below editor
      val editorResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest(
            permissionType = PermissionType.WORKSPACE_EDITOR,
            userId = userId,
            workspaceIds = listOf<UUID>(workspaceId, otherWorkspaceId),
          ),
        )

      Assertions.assertEquals(PermissionCheckRead.Status.FAILED, editorResult.status)

      // READER succeeds because both workspaces have at least READER permissions
      val readerResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest(
            permissionType = PermissionType.WORKSPACE_READER,
            userId = userId,
            workspaceIds = listOf<UUID>(workspaceId, otherWorkspaceId),
          ),
        )

      Assertions.assertEquals(PermissionCheckRead.Status.SUCCEEDED, readerResult.status)
    }

    @Test
    fun permissionsCheckMultipleWorkspacesOrgPermission() {
      val otherWorkspaceId = UUID.randomUUID()
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withUserId(userId)
            .withWorkspaceId(workspaceId),
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_READER)
            .withUserId(userId)
            .withOrganizationId(organizationId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(workspaceId)))

      // otherWorkspace is in the user's organization, so the user's Org Reader permission should apply
      whenever(workspaceService.getStandardWorkspaceNoSecrets(otherWorkspaceId, false))
        .thenReturn(StandardWorkspace().withOrganizationId(organizationId))

      // EDITOR fails because READER is below editor
      val editorResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest(
            permissionType = PermissionType.WORKSPACE_EDITOR,
            userId = userId,
            workspaceIds = listOf<UUID>(workspaceId, otherWorkspaceId),
          ),
        )

      Assertions.assertEquals(PermissionCheckRead.Status.FAILED, editorResult.status)

      // READER succeeds because both workspaces have at least READER permissions
      val readerResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest(
            permissionType = PermissionType.WORKSPACE_READER,
            userId = userId,
            workspaceIds = listOf<UUID>(workspaceId, otherWorkspaceId),
          ),
        )

      Assertions.assertEquals(PermissionCheckRead.Status.SUCCEEDED, readerResult.status)
    }

    @Test
    fun workspaceNotInOrganization() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
        ),
      )

      val workspace = mock<StandardWorkspace>()
      whenever(workspace.organizationId).thenReturn(UUID.randomUUID()) // different organization
      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(workspace)

      val request =
        PermissionCheckRequest(
          permissionType = PermissionType.WORKSPACE_ADMIN,
          userId = userId,
          workspaceId = workspaceId,
        )

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.Status.FAILED, result.status)
    }

    @ParameterizedTest
    @EnumSource(
      value = Permission.PermissionType::class,
      names = [
        "WORKSPACE_OWNER",
        "WORKSPACE_ADMIN",
        "WORKSPACE_EDITOR",
        "WORKSPACE_SOURCE_EDITOR",
        "WORKSPACE_DESTINATION_EDITOR",
        "WORKSPACE_READER",
      ],
    )
    fun workspaceLevelPermissions(userPermissionType: Permission.PermissionType?) {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(userPermissionType)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(workspaceId)))

      if (userPermissionType == Permission.PermissionType.WORKSPACE_OWNER) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_ADMIN) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_EDITOR) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_SOURCE_EDITOR ||
        userPermissionType == Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR
      ) {
        val oppositeActorEditor =
          if (userPermissionType == Permission.PermissionType.WORKSPACE_SOURCE_EDITOR) {
            Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR
          } else {
            Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
          }

        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(userPermissionType)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_RUNNER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        // The opposite actor type's editor, and the full workspace editor, are both out of reach.
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(oppositeActorEditor)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_READER) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_EDITOR,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }
    }

    @ParameterizedTest
    @EnumSource(
      value = Permission.PermissionType::class,
      names = ["ORGANIZATION_ADMIN", "ORGANIZATION_EDITOR", "ORGANIZATION_READER", "ORGANIZATION_MEMBER"],
    )
    fun organizationLevelPermissions(userPermissionType: Permission.PermissionType?) {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(userPermissionType)
            .withOrganizationId(organizationId)
            .withUserId(userId),
        ),
      )

      val workspace = mock<StandardWorkspace>()
      whenever(workspace.organizationId).thenReturn(organizationId)
      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(workspace)

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_ADMIN) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_EDITOR) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_READER) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_EDITOR,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_MEMBER) {
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_EDITOR,
              ),
            ).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
        )

        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
        )
        Assertions.assertEquals(
          PermissionCheckRead.Status.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
        )
      }
    }

    @Test
    fun instanceAdminPermissions() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
            .withUserId(userId),
        ),
      )

      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler
          .checkPermissions(
            PermissionCheckRequest(permissionType = PermissionType.INSTANCE_ADMIN, userId = userId),
          ).status,
      )

      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler
          .checkPermissions(
            getWorkspacePermissionCheck(
              Permission.PermissionType.WORKSPACE_ADMIN,
            ),
          ).status,
      )
      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).status,
      )
      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).status,
      )

      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).status,
      )
      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).status,
      )
      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).status,
      )
      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).status,
      )
    }

    @Test
    fun ensureAllPermissionTypesAreCovered() {
      val coveredPermissionTypes =
        setOf(
          Permission.PermissionType.INSTANCE_ADMIN,
          Permission.PermissionType.WORKSPACE_OWNER,
          Permission.PermissionType.WORKSPACE_ADMIN,
          Permission.PermissionType.WORKSPACE_EDITOR,
          Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
          Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
          Permission.PermissionType.ORGANIZATION_ADMIN,
          Permission.PermissionType.ORGANIZATION_EDITOR,
          Permission.PermissionType.ORGANIZATION_RUNNER,
          Permission.PermissionType.ORGANIZATION_READER,
          Permission.PermissionType.ORGANIZATION_MEMBER,
          Permission.PermissionType.DATAPLANE,
        )

      // If this assertion fails, it means a new PermissionType was added! Please update either the
      // `organizationLevelPermissions` or `workspaceLeveLPermissions` tests above this one to
      // cover the new PermissionType. Once you've made sure that your new PermissionType is
      // covered, you can add it to the `coveredPermissionTypes` list above in order to make this
      // assertion pass.
      Assertions.assertEquals(coveredPermissionTypes, setOf(*Permission.PermissionType.entries.toTypedArray()))
    }

    @Test
    fun ensureNoExceptionOnOrgPermissionCheckForWorkspaceOutsideTheOrg() {
      // Ensure that when we check permissions for a workspace that's not in an organization against an
      // org permission, we don't throw an exception.
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(workspaceId)))

      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false))
        .thenReturn(StandardWorkspace().withWorkspaceId(workspaceId))

      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler
          .checkPermissions(
            PermissionCheckRequest(
              permissionType = PermissionType.WORKSPACE_ADMIN,
              workspaceId = workspaceId,
              userId = userId,
            ),
          ).status,
      )
    }

    @Test
    fun ensureFailedPermissionCheckForWorkspaceOutsideTheOrg() {
      // Ensure that when we check permissions for a workspace that's not in an organization against an
      // org permission, we fail the check if the workspace has no org ID set
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(workspaceId)))

      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false))
        .thenReturn(StandardWorkspace().withWorkspaceId(workspaceId))

      Assertions.assertEquals(
        PermissionCheckRead.Status.FAILED,
        permissionHandler
          .checkPermissions(
            PermissionCheckRequest(
              permissionType = PermissionType.ORGANIZATION_ADMIN,
              workspaceId = workspaceId,
              userId = userId,
            ),
          ).status,
      )
    }

    @Test
    fun `checkPermissions succeeds with a group-derived permission`() {
      // group-derived rows have a null userId on the underlying permission row; the effective
      // projection stamps the requested userId so the mismatched-userId guard still passes.
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withGroupId(UUID.randomUUID()),
        ),
      )

      Assertions.assertEquals(
        PermissionCheckRead.Status.SUCCEEDED,
        permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).status,
      )
    }

    @Test
    fun `checkPermissions fails when an effective permission belongs to a different user`() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(UUID.randomUUID()), // direct row owned by another user
        ),
      )

      Assertions.assertEquals(
        PermissionCheckRead.Status.FAILED,
        permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).status,
      )
    }

    @Test
    fun `effectivePermissionReadListForUser stamps userId only on group-derived rows`() {
      val groupId = UUID.randomUUID()
      val groupPermissionId = UUID.randomUUID()
      val directPermissionId = UUID.randomUUID()
      val directOwnerId = UUID.randomUUID()
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionId(groupPermissionId)
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withGroupId(groupId),
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionId(directPermissionId)
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(directOwnerId),
        ),
      )

      val reads = permissionHandler.effectivePermissionReadListForUser(userId).permissions

      val groupRead = reads.single { it.permissionId == groupPermissionId }
      Assertions.assertEquals(userId, groupRead.userId)
      Assertions.assertEquals(groupId, groupRead.groupId)
      Assertions.assertEquals(workspaceId, groupRead.workspaceId)

      val directRead = reads.single { it.permissionId == directPermissionId }
      Assertions.assertEquals(directOwnerId, directRead.userId)
      Assertions.assertNull(directRead.groupId)
    }

    @Test
    fun `permissionReadListForUser still uses direct permissions`() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(workspaceId), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(workspaceId)))

      val result = permissionHandler.permissionReadListForUser(userId)

      Assertions.assertEquals(1, result.permissions.size)
      verify(permissionService, times(0)).getEffectivePermissionsForUser(anyOrNull())
    }

    @Test
    fun getPermissionsByServiceAccountIdReturnsPermissions() {
      val serviceAccountId = UUID.randomUUID()
      val expected =
        listOf(
          Permission()
            .withPermissionId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.DATAPLANE)
            .withServiceAccountId(serviceAccountId),
        )

      whenever(permissionService.getPermissionsByServiceAccountId(serviceAccountId))
        .thenReturn(expected)

      Assertions.assertEquals(expected, permissionHandler.getPermissionsByServiceAccountId(serviceAccountId))
    }

    @Test
    fun `permissionsCheckMultipleWorkspaces throws when workspaceIds is missing`() {
      Assertions.assertThrows(NullPointerException::class.java) {
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest(
            permissionType = PermissionType.WORKSPACE_READER,
            userId = userId,
            workspaceIds = null,
          ),
        )
      }

      verify(permissionService, times(0)).getEffectivePermissionsForUser(anyOrNull())
    }

    private fun getWorkspacePermissionCheck(targetPermissionType: Permission.PermissionType): PermissionCheckRequest =
      PermissionCheckRequest(
        permissionType = targetPermissionType.convertTo<PermissionType>(),
        userId = userId,
        workspaceId = workspaceId,
      )

    private fun getOrganizationPermissionCheck(targetPermissionType: Permission.PermissionType): PermissionCheckRequest =
      PermissionCheckRequest(
        permissionType = targetPermissionType.convertTo<PermissionType>(),
        userId = userId,
        organizationId = organizationId,
      )
  }

  @Nested
  internal inner class PermissionReadListForUser {
    private val userId: UUID = UUID.randomUUID()
    private val liveWorkspaceId: UUID = UUID.randomUUID()
    private val tombstonedWorkspaceId1: UUID = UUID.randomUUID()
    private val tombstonedWorkspaceId2: UUID = UUID.randomUUID()

    @Test
    fun filtersOutPermissionsForTombstonedWorkspaces() {
      val liveWorkspacePermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withWorkspaceId(liveWorkspaceId)
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
      val tombstonedWorkspacePermission1 =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withWorkspaceId(tombstonedWorkspaceId1)
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
      val tombstonedWorkspacePermission2 =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withWorkspaceId(tombstonedWorkspaceId2)
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
      val organizationPermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withOrganizationId(UUID.randomUUID())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
      val instancePermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)

      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          liveWorkspacePermission,
          tombstonedWorkspacePermission1,
          tombstonedWorkspacePermission2,
          organizationPermission,
          instancePermission,
        ),
      )
      whenever(workspaceService.listStandardWorkspacesWithIds(listOf(liveWorkspaceId, tombstonedWorkspaceId1, tombstonedWorkspaceId2), false))
        .thenReturn(listOf(StandardWorkspace().withWorkspaceId(liveWorkspaceId)))

      val result = permissionHandler.permissionReadListForUser(userId)

      Assertions.assertEquals(
        setOf(liveWorkspacePermission.permissionId, organizationPermission.permissionId, instancePermission.permissionId),
        result.permissions.map { it.permissionId }.toSet(),
      )
      verify(workspaceService, times(1))
        .listStandardWorkspacesWithIds(listOf(liveWorkspaceId, tombstonedWorkspaceId1, tombstonedWorkspaceId2), false)
    }

    @Test
    fun skipsWorkspaceLookupWhenNoWorkspacePermissions() {
      val organizationPermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withOrganizationId(UUID.randomUUID())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
      val instancePermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)

      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(organizationPermission, instancePermission),
      )

      val result = permissionHandler.permissionReadListForUser(userId)

      Assertions.assertEquals(2, result.permissions.size)
      verify(workspaceService, times(0)).listStandardWorkspacesWithIds(anyOrNull(), eq(false))
    }

    @Test
    fun returnsEmptyListWhenUserHasNoPermissions() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(emptyList())

      val result = permissionHandler.permissionReadListForUser(userId)

      Assertions.assertTrue(result.permissions.isEmpty())
      verify(workspaceService, times(0)).listStandardWorkspacesWithIds(anyOrNull(), eq(false))
    }
  }

  @Nested
  internal inner class GroupOwnedPermissionAccess {
    private val userId: UUID = UUID.randomUUID()
    private val workspaceId: UUID = UUID.randomUUID()
    private val permissionId: UUID = UUID.randomUUID()

    private val groupOwnedPermission: Permission =
      Permission()
        .withPermissionId(permissionId)
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
        .withWorkspaceId(workspaceId)
        .withGroupId(UUID.randomUUID())

    @Test
    fun `getPermissionRead rejects group-owned permission`() {
      whenever(permissionService.getPermission(permissionId)).thenReturn(groupOwnedPermission)

      Assertions.assertThrows(OperationNotAllowedException::class.java) {
        permissionHandler.getPermissionRead(PermissionIdRequestBody(permissionId = permissionId))
      }
    }

    @Test
    fun `getPermissionRead rejects service account permission`() {
      whenever(permissionService.getPermission(permissionId)).thenReturn(
        Permission()
          .withPermissionId(permissionId)
          .withPermissionType(Permission.PermissionType.DATAPLANE)
          .withServiceAccountId(UUID.randomUUID()),
      )

      Assertions.assertThrows(OperationNotAllowedException::class.java) {
        permissionHandler.getPermissionRead(PermissionIdRequestBody(permissionId = permissionId))
      }
    }

    @Test
    fun `updatePermission rejects group-owned permission`() {
      whenever(permissionService.getPermission(permissionId)).thenReturn(groupOwnedPermission)

      Assertions.assertThrows(OperationNotAllowedException::class.java) {
        permissionHandler.updatePermission(
          PermissionUpdate(permissionId = permissionId, permissionType = PermissionType.WORKSPACE_ADMIN),
        )
      }

      verify(permissionService, times(0)).updatePermission(anyOrNull())
    }

    @Test
    fun `permissionId returned by effectivePermissionReadListForUser is rejected by getPermissionRead`() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(listOf<Permission>(groupOwnedPermission))
      whenever(permissionService.getPermission(permissionId)).thenReturn(groupOwnedPermission)

      val listed = permissionHandler.effectivePermissionReadListForUser(userId).permissions.single()
      Assertions.assertEquals(permissionId, listed.permissionId)

      Assertions.assertThrows(OperationNotAllowedException::class.java) {
        permissionHandler.getPermissionRead(PermissionIdRequestBody(permissionId = listed.permissionId))
      }
    }

    @Test
    fun `permissionId returned by effectivePermissionReadListForUser is rejected by updatePermission`() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(listOf<Permission>(groupOwnedPermission))
      whenever(permissionService.getPermission(permissionId)).thenReturn(groupOwnedPermission)

      val listed = permissionHandler.effectivePermissionReadListForUser(userId).permissions.single()
      Assertions.assertEquals(permissionId, listed.permissionId)

      Assertions.assertThrows(OperationNotAllowedException::class.java) {
        permissionHandler.updatePermission(
          PermissionUpdate(
            permissionId = listed.permissionId,
            permissionType = PermissionType.WORKSPACE_ADMIN,
          ),
        )
      }

      verify(permissionService, times(0)).updatePermission(anyOrNull())
    }

    @Test
    fun `permissionId returned by effectivePermissionReadListForUser is rejected by deletePermission`() {
      whenever(permissionService.getEffectivePermissionsForUser(userId)).thenReturn(listOf<Permission>(groupOwnedPermission))
      whenever(permissionService.getPermission(permissionId)).thenReturn(groupOwnedPermission)

      val listed = permissionHandler.effectivePermissionReadListForUser(userId).permissions.single()
      Assertions.assertEquals(permissionId, listed.permissionId)

      Assertions.assertThrows(OperationNotAllowedException::class.java) {
        permissionHandler.deletePermission(PermissionIdRequestBody(permissionId = listed.permissionId))
      }

      verify(permissionService, times(0)).deletePermission(anyOrNull())
    }
  }

  @Nested
  internal inner class DeleteUserFromWorkspace {
    private val workspaceId: UUID = UUID.randomUUID()
    private val userId: UUID = UUID.randomUUID()

    @Test
    fun testDeleteUserFromWorkspace() {
      // should be deleted
      val workspacePermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withWorkspaceId(workspaceId)
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

      // should not be deleted, different workspace
      val otherWorkspacePermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withWorkspaceId(UUID.randomUUID())
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

      // should not be deleted, org permission
      val orgPermission =
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withOrganizationId(UUID.randomUUID())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(workspacePermission, otherWorkspacePermission, orgPermission),
      )

      permissionHandler.deleteUserFromWorkspace(PermissionDeleteUserFromWorkspaceRequestBody(userId = userId, workspaceId = workspaceId))

      // verify the intended permission was deleted
      verify(permissionService).deletePermissions(listOf<UUID>(workspacePermission.permissionId))
      verify(permissionService, times(1)).deletePermissions(anyOrNull())
    }
  }

  @Nested
  internal inner class CountInstanceEditors {
    @Test
    fun actorScopedWorkspaceEditorsCountAsEditors() {
      val sourceEditorUserId = UUID.randomUUID()
      val destinationEditorUserId = UUID.randomUUID()

      whenever(permissionService.listPermissions()).thenReturn(
        listOf<Permission>(
          Permission()
            .withUserId(sourceEditorUserId)
            .withWorkspaceId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_SOURCE_EDITOR),
          Permission()
            .withUserId(destinationEditorUserId)
            .withWorkspaceId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR),
        ),
      )

      Assertions.assertEquals(2, permissionHandler.countInstanceEditors())
    }

    @Test
    fun readersDoNotCountAsEditors() {
      whenever(permissionService.listPermissions()).thenReturn(
        listOf<Permission>(
          Permission()
            .withUserId(UUID.randomUUID())
            .withWorkspaceId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_READER),
          Permission()
            .withUserId(UUID.randomUUID())
            .withOrganizationId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_READER),
        ),
      )

      Assertions.assertEquals(0, permissionHandler.countInstanceEditors())
    }

    @Test
    fun oneUserWithMultipleEditorRolesCountsOnce() {
      val userId = UUID.randomUUID()

      whenever(permissionService.listPermissions()).thenReturn(
        listOf<Permission>(
          Permission()
            .withUserId(userId)
            .withWorkspaceId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_SOURCE_EDITOR),
          Permission()
            .withUserId(userId)
            .withWorkspaceId(UUID.randomUUID())
            .withPermissionType(Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR),
        ),
      )

      Assertions.assertEquals(1, permissionHandler.countInstanceEditors())
    }
  }
}
