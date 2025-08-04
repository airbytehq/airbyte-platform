/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionCheckRequest
import io.airbyte.api.model.generated.PermissionDeleteUserFromWorkspaceRequestBody
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.PermissionUpdate
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.data.ConfigNotFoundException
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
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
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
    @Throws(Exception::class)
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
        .withUserId(user.getUserId())
        .withWorkspaceId(UUID.randomUUID())
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)

    private val permissionOrganizationAdmin: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(user.getUserId())
        .withOrganizationId(organizationId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
      whenever(permissionService.getPermission(permissionWorkspaceReader.getPermissionId()))
        .thenReturn(
          Permission()
            .withPermissionId(permissionWorkspaceReader.getPermissionId())
            .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
            .withWorkspaceId(permissionWorkspaceReader.getWorkspaceId())
            .withUserId(permissionWorkspaceReader.getUserId()),
        )

      whenever(permissionService.getPermission(permissionOrganizationAdmin.getPermissionId()))
        .thenReturn(
          Permission()
            .withPermissionId(permissionOrganizationAdmin.getPermissionId())
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(permissionOrganizationAdmin.getOrganizationId())
            .withUserId(permissionOrganizationAdmin.getUserId()),
        )
    }

    @Test
    @Throws(Exception::class)
    fun updatesPermission() {
      val update =
        PermissionUpdate()
          .permissionId(permissionWorkspaceReader.getPermissionId())
          .permissionType(PermissionType.WORKSPACE_ADMIN) // changing to workspace_admin

      permissionHandler.updatePermission(update)

      verify(permissionService).updatePermission(
        Permission()
          .withPermissionId(permissionWorkspaceReader.getPermissionId())
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withUserId(permissionWorkspaceReader.getUserId())
          .withWorkspaceId(permissionWorkspaceReader.getWorkspaceId())
          .withOrganizationId(null),
      )
    }

    @Test
    fun testUpdateToInstanceAdminPermissionThrows() {
      val permissionUpdate =
        PermissionUpdate()
          .permissionType(PermissionType.INSTANCE_ADMIN)
          .permissionId(permissionOrganizationAdmin.getPermissionId())
      Assertions.assertThrows(
        JsonValidationException::class.java,
      ) { permissionHandler.updatePermission(permissionUpdate) }
    }

    @Test
    @Throws(Exception::class)
    fun throwsConflictExceptionIfServiceBlocksUpdate() {
      val update =
        PermissionUpdate()
          .permissionId(permissionOrganizationAdmin.getPermissionId())
          .permissionType(PermissionType.ORGANIZATION_EDITOR) // changing to organization_editor

      doThrow(RemoveLastOrgAdminPermissionException::class)
        .whenever(permissionService)
        .updatePermission(anyOrNull())
      Assertions.assertThrows(ConflictException::class.java) { permissionHandler.updatePermission(update) }
    }

    @Test
    @Throws(Exception::class)
    fun workspacePermissionUpdatesDoNotModifyIdFields() {
      val workspacePermissionUpdate =
        PermissionUpdate()
          .permissionId(permissionWorkspaceReader.getPermissionId())
          .permissionType(PermissionType.WORKSPACE_EDITOR) // changing to workspace_editor

      permissionHandler.updatePermission(workspacePermissionUpdate)

      verify(permissionService).updatePermission(
        Permission()
          .withPermissionId(permissionWorkspaceReader.getPermissionId())
          .withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
          .withWorkspaceId(permissionWorkspaceReader.getWorkspaceId()) // workspace ID preserved from original permission
          .withUserId(permissionWorkspaceReader.getUserId()),
      ) // user ID preserved from original permission
    }

    @Test
    @Throws(Exception::class)
    fun organizationPermissionUpdatesDoNotModifyIdFields() {
      val orgPermissionUpdate =
        PermissionUpdate()
          .permissionId(permissionOrganizationAdmin.getPermissionId())
          .permissionType(PermissionType.ORGANIZATION_EDITOR) // changing to organization_editor

      permissionHandler.updatePermission(orgPermissionUpdate)

      verify(permissionService).updatePermission(
        Permission()
          .withPermissionId(permissionOrganizationAdmin.getPermissionId())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_EDITOR)
          .withOrganizationId(permissionOrganizationAdmin.getOrganizationId()) // organization ID preserved from original permission
          .withUserId(permissionOrganizationAdmin.getUserId()),
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
        .withUserId(user.getUserId())
        .withWorkspaceId(UUID.randomUUID())
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)

    private val permissionOrganizationAdmin: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(user.getUserId())
        .withOrganizationId(organizationId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

    @Test
    @Throws(Exception::class)
    fun deletesPermission() {
      permissionHandler.deletePermission(PermissionIdRequestBody().permissionId(permissionWorkspaceReader.getPermissionId()))

      verify(permissionService).deletePermission(permissionWorkspaceReader.getPermissionId())
    }

    @Test
    @Throws(Exception::class)
    fun throwsConflictIfPersistenceBlocks() {
      doThrow(RemoveLastOrgAdminPermissionException::class)
        .whenever(permissionService)
        .deletePermission(anyOrNull())

      Assertions.assertThrows(ConflictException::class.java) {
        permissionHandler.deletePermission(
          PermissionIdRequestBody().permissionId(permissionOrganizationAdmin.getPermissionId()),
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
    @Throws(IOException::class)
    fun mismatchedUserId() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withUserId(userId),
        ),
      )

      val request =
        PermissionCheckRequest()
          .permissionType(PermissionType.WORKSPACE_ADMIN)
          .userId(UUID.randomUUID()) // different user
          .workspaceId(workspaceId)

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.FAILED, result.getStatus())
    }

    @Test
    @Throws(IOException::class)
    fun mismatchedWorkspaceId() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      val request =
        PermissionCheckRequest()
          .permissionType(PermissionType.WORKSPACE_ADMIN)
          .userId(userId)
          .workspaceId(UUID.randomUUID()) // different workspace

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.FAILED, result.getStatus())
    }

    @Test
    @Throws(IOException::class)
    fun mismatchedOrganizationId() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
        ),
      )

      val request =
        PermissionCheckRequest()
          .permissionType(PermissionType.ORGANIZATION_ADMIN)
          .userId(userId)
          .organizationId(UUID.randomUUID()) // different organization

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.FAILED, result.getStatus())
    }

    @Test
    @Throws(IOException::class)
    fun permissionsCheckMultipleWorkspaces() {
      val otherWorkspaceId = UUID.randomUUID()
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withUserId(userId)
            .withWorkspaceId(workspaceId),
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
            .withUserId(userId)
            .withWorkspaceId(otherWorkspaceId),
        ),
      )

      // EDITOR fails because READER is below editor
      val editorResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest()
            .permissionType(PermissionType.WORKSPACE_EDITOR)
            .userId(userId)
            .workspaceIds(listOf<UUID?>(workspaceId, otherWorkspaceId)),
        )

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.FAILED, editorResult.getStatus())

      // READER succeeds because both workspaces have at least READER permissions
      val readerResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest()
            .permissionType(PermissionType.WORKSPACE_READER)
            .userId(userId)
            .workspaceIds(listOf<UUID?>(workspaceId, otherWorkspaceId)),
        )

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.SUCCEEDED, readerResult.getStatus())
    }

    @Test
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun permissionsCheckMultipleWorkspacesOrgPermission() {
      val otherWorkspaceId = UUID.randomUUID()
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withUserId(userId)
            .withWorkspaceId(workspaceId),
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_READER)
            .withUserId(userId)
            .withOrganizationId(organizationId),
        ),
      )

      // otherWorkspace is in the user's organization, so the user's Org Reader permission should apply
      whenever(workspaceService.getStandardWorkspaceNoSecrets(otherWorkspaceId, false))
        .thenReturn(StandardWorkspace().withOrganizationId(organizationId))

      // EDITOR fails because READER is below editor
      val editorResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest()
            .permissionType(PermissionType.WORKSPACE_EDITOR)
            .userId(userId)
            .workspaceIds(listOf<UUID?>(workspaceId, otherWorkspaceId)),
        )

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.FAILED, editorResult.getStatus())

      // READER succeeds because both workspaces have at least READER permissions
      val readerResult =
        permissionHandler.permissionsCheckMultipleWorkspaces(
          PermissionsCheckMultipleWorkspacesRequest()
            .permissionType(PermissionType.WORKSPACE_READER)
            .userId(userId)
            .workspaceIds(listOf<UUID?>(workspaceId, otherWorkspaceId)),
        )

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.SUCCEEDED, readerResult.getStatus())
    }

    @Test
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun workspaceNotInOrganization() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
        ),
      )

      val workspace = mock<StandardWorkspace>()
      whenever(workspace.getOrganizationId()).thenReturn(UUID.randomUUID()) // different organization
      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(workspace)

      val request =
        PermissionCheckRequest()
          .permissionType(PermissionType.WORKSPACE_ADMIN)
          .userId(userId)
          .workspaceId(workspaceId)

      val result = permissionHandler.checkPermissions(request)

      Assertions.assertEquals(PermissionCheckRead.StatusEnum.FAILED, result.getStatus())
    }

    @ParameterizedTest
    @EnumSource(value = Permission.PermissionType::class, names = ["WORKSPACE_OWNER", "WORKSPACE_ADMIN", "WORKSPACE_EDITOR", "WORKSPACE_READER"])
    @Throws(
      IOException::class,
    )
    fun workspaceLevelPermissions(userPermissionType: Permission.PermissionType?) {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(userPermissionType)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      if (userPermissionType == Permission.PermissionType.WORKSPACE_OWNER) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_ADMIN) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_EDITOR) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }

      if (userPermissionType == Permission.PermissionType.WORKSPACE_READER) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_EDITOR,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }
    }

    @ParameterizedTest
    @EnumSource(
      value = Permission.PermissionType::class,
      names = ["ORGANIZATION_ADMIN", "ORGANIZATION_EDITOR", "ORGANIZATION_READER", "ORGANIZATION_MEMBER"],
    )
    @Throws(
      IOException::class,
      JsonValidationException::class,
      ConfigNotFoundException::class,
    )
    fun organizationLevelPermissions(userPermissionType: Permission.PermissionType?) {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(userPermissionType)
            .withOrganizationId(organizationId)
            .withUserId(userId),
        ),
      )

      val workspace = mock<StandardWorkspace>()
      whenever(workspace.getOrganizationId()).thenReturn(organizationId)
      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(workspace)

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_ADMIN) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_EDITOR) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_READER) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_EDITOR,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }

      if (userPermissionType == Permission.PermissionType.ORGANIZATION_MEMBER) {
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_ADMIN,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler
            .checkPermissions(
              getWorkspacePermissionCheck(
                Permission.PermissionType.WORKSPACE_EDITOR,
              ),
            ).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
        )

        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.FAILED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
        )
        Assertions.assertEquals(
          PermissionCheckRead.StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
        )
      }
    }

    @Test
    @Throws(IOException::class)
    fun instanceAdminPermissions() {
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
            .withUserId(userId),
        ),
      )

      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler
          .checkPermissions(
            PermissionCheckRequest()
              .permissionType(PermissionType.INSTANCE_ADMIN)
              .userId(userId),
          ).getStatus(),
      )

      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler
          .checkPermissions(
            getWorkspacePermissionCheck(
              Permission.PermissionType.WORKSPACE_ADMIN,
            ),
          ).getStatus(),
      )
      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_EDITOR)).getStatus(),
      )
      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler.checkPermissions(getWorkspacePermissionCheck(Permission.PermissionType.WORKSPACE_READER)).getStatus(),
      )

      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_ADMIN)).getStatus(),
      )
      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_EDITOR)).getStatus(),
      )
      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_READER)).getStatus(),
      )
      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler.checkPermissions(getOrganizationPermissionCheck(Permission.PermissionType.ORGANIZATION_MEMBER)).getStatus(),
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
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun ensureNoExceptionOnOrgPermissionCheckForWorkspaceOutsideTheOrg() {
      // Ensure that when we check permissions for a workspace that's not in an organization against an
      // org permission, we don't throw an exception.
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false))
        .thenReturn(StandardWorkspace().withWorkspaceId(workspaceId))

      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.SUCCEEDED,
        permissionHandler
          .checkPermissions(
            PermissionCheckRequest()
              .permissionType(PermissionType.WORKSPACE_ADMIN)
              .workspaceId(workspaceId)
              .userId(userId),
          ).getStatus(),
      )
    }

    @Test
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun ensureFailedPermissionCheckForWorkspaceOutsideTheOrg() {
      // Ensure that when we check permissions for a workspace that's not in an organization against an
      // org permission, we fail the check if the workspace has no org ID set
      whenever(permissionService.getPermissionsForUser(userId)).thenReturn(
        listOf<Permission>(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organizationId)
            .withUserId(userId),
          Permission()
            .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
            .withWorkspaceId(workspaceId)
            .withUserId(userId),
        ),
      )

      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false))
        .thenReturn(StandardWorkspace().withWorkspaceId(workspaceId))

      Assertions.assertEquals(
        PermissionCheckRead.StatusEnum.FAILED,
        permissionHandler
          .checkPermissions(
            PermissionCheckRequest()
              .permissionType(PermissionType.ORGANIZATION_ADMIN)
              .workspaceId(workspaceId)
              .userId(userId),
          ).getStatus(),
      )
    }

    @Test
    fun getPermissionsByServiceAccountIdReturnsPermissions() {
      val serviceAccountId = UUID.randomUUID()
      val expected =
        listOf(
          Permission()
            .withPermissionType(Permission.PermissionType.DATAPLANE)
            .withServiceAccountId(serviceAccountId),
        )

      whenever(permissionService.getPermissionsByServiceAccountId(serviceAccountId))
        .thenReturn(expected)

      Assertions.assertEquals(expected, permissionHandler.getPermissionsByServiceAccountId(serviceAccountId))
    }

    private fun getWorkspacePermissionCheck(targetPermissionType: Permission.PermissionType): PermissionCheckRequest =
      PermissionCheckRequest()
        .permissionType(targetPermissionType.convertTo<PermissionType>())
        .userId(userId)
        .workspaceId(workspaceId)

    private fun getOrganizationPermissionCheck(targetPermissionType: Permission.PermissionType): PermissionCheckRequest =
      PermissionCheckRequest()
        .permissionType(targetPermissionType.convertTo<PermissionType>())
        .userId(userId)
        .organizationId(organizationId)
  }

  @Nested
  internal inner class DeleteUserFromWorkspace {
    private val workspaceId: UUID = UUID.randomUUID()
    private val userId: UUID = UUID.randomUUID()

    @Test
    @Throws(Exception::class)
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

      permissionHandler.deleteUserFromWorkspace(PermissionDeleteUserFromWorkspaceRequestBody().userId(userId).workspaceId(workspaceId))

      // verify the intended permission was deleted
      verify(permissionService).deletePermissions(listOf<UUID>(workspacePermission.getPermissionId()))
      verify(permissionService, times(1)).deletePermissions(anyOrNull())
    }
  }
}
