/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionDeleteUserFromWorkspaceRequestBody;
import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionUpdate;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.errors.ConflictException;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.PermissionDao;
import io.airbyte.data.services.RemoveLastOrgAdminPermissionException;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class PermissionHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private PermissionPersistence permissionPersistence;
  private WorkspaceService workspaceService;
  private PermissionHandler permissionHandler;
  private PermissionDao permissionDao;

  @BeforeEach
  void setUp() {
    permissionPersistence = mock(PermissionPersistence.class);
    uuidSupplier = mock(Supplier.class);
    workspaceService = mock(WorkspaceService.class);
    permissionDao = mock(PermissionDao.class);
    permissionHandler = new PermissionHandler(permissionPersistence, workspaceService, uuidSupplier, permissionDao);
  }

  @Test
  void isUserInstanceAdmin() throws IOException {
    final UUID userId = UUID.randomUUID();

    when(permissionPersistence.isUserInstanceAdmin(userId)).thenReturn(true);
    Assertions.assertTrue(permissionHandler.isUserInstanceAdmin(userId));

    when(permissionPersistence.isUserInstanceAdmin(userId)).thenReturn(false);
    Assertions.assertFalse(permissionHandler.isUserInstanceAdmin(userId));
  }

  @Nested
  class CreatePermission {

    private static final UUID USER_ID = UUID.randomUUID();
    private static final UUID WORKSPACE_ID = UUID.randomUUID();
    private static final UUID PERMISSION_ID = UUID.randomUUID();
    private static final Permission PERMISSION = new Permission()
        .withPermissionId(PERMISSION_ID)
        .withUserId(USER_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withPermissionType(PermissionType.WORKSPACE_ADMIN);

    @Test
    void testCreatePermission() throws Exception {
      final List<Permission> existingPermissions = List.of();
      when(permissionDao.getPermissionsForUser(any())).thenReturn(existingPermissions);
      when(uuidSupplier.get()).thenReturn(PERMISSION_ID);
      final Permission permissionCreate = new Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_OWNER)
          .withUserId(USER_ID)
          .withWorkspaceId(WORKSPACE_ID);
      when(permissionDao.createPermission(any())).thenReturn(PERMISSION);
      final Permission actual = permissionHandler.createPermission(permissionCreate);
      final Permission expected = new Permission()
          .withPermissionId(PERMISSION_ID)
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withUserId(USER_ID)
          .withWorkspaceId(WORKSPACE_ID);

      assertEquals(expected, actual);
    }

    @Test
    void testCreateInstanceAdminPermissionThrows() {
      final Permission permissionCreate = new Permission()
          .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
          .withUserId(USER_ID);
      assertThrows(JsonValidationException.class, () -> permissionHandler.createPermission(permissionCreate));
    }

  }

  @Nested
  class UpdatePermission {

    private static final UUID ORGANIZATION_ID = UUID.randomUUID();

    private static final AuthenticatedUser USER = new AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withName("User")
        .withEmail("user@email.com");

    private static final Permission PERMISSION_WORKSPACE_READER = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(USER.getUserId())
        .withWorkspaceId(UUID.randomUUID())
        .withPermissionType(PermissionType.WORKSPACE_READER);

    private static final Permission PERMISSION_ORGANIZATION_ADMIN = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(USER.getUserId())
        .withOrganizationId(ORGANIZATION_ID)
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN);

    @BeforeEach
    void setup() throws IOException {
      when(permissionPersistence.getPermission(PERMISSION_WORKSPACE_READER.getPermissionId()))
          .thenReturn(Optional.of(new Permission()
              .withPermissionId(PERMISSION_WORKSPACE_READER.getPermissionId())
              .withPermissionType(PermissionType.WORKSPACE_READER)
              .withWorkspaceId(PERMISSION_WORKSPACE_READER.getWorkspaceId())
              .withUserId(PERMISSION_WORKSPACE_READER.getUserId())));

      when(permissionPersistence.getPermission(PERMISSION_ORGANIZATION_ADMIN.getPermissionId()))
          .thenReturn(Optional.of(new Permission()
              .withPermissionId(PERMISSION_ORGANIZATION_ADMIN.getPermissionId())
              .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
              .withOrganizationId(PERMISSION_ORGANIZATION_ADMIN.getOrganizationId())
              .withUserId(PERMISSION_ORGANIZATION_ADMIN.getUserId())));
    }

    @Test
    void updatesPermission() throws Exception {
      final PermissionUpdate update = new PermissionUpdate()
          .permissionId(PERMISSION_WORKSPACE_READER.getPermissionId())
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN); // changing to workspace_admin

      permissionHandler.updatePermission(update);

      verify(permissionDao).updatePermission(new Permission()
          .withPermissionId(PERMISSION_WORKSPACE_READER.getPermissionId())
          .withPermissionType(PermissionType.WORKSPACE_ADMIN)
          .withUserId(PERMISSION_WORKSPACE_READER.getUserId())
          .withWorkspaceId(PERMISSION_WORKSPACE_READER.getWorkspaceId())
          .withOrganizationId(null));
    }

    @Test
    void testUpdateToInstanceAdminPermissionThrows() {
      final PermissionUpdate permissionUpdate = new PermissionUpdate()
          .permissionType(io.airbyte.api.model.generated.PermissionType.INSTANCE_ADMIN)
          .permissionId(PERMISSION_ORGANIZATION_ADMIN.getPermissionId());
      assertThrows(JsonValidationException.class, () -> permissionHandler.updatePermission(permissionUpdate));
    }

    @Test
    void throwsConflictExceptionIfServiceBlocksUpdate() throws Exception {
      final PermissionUpdate update = new PermissionUpdate()
          .permissionId(PERMISSION_ORGANIZATION_ADMIN.getPermissionId())
          .permissionType(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_EDITOR); // changing to organization_editor

      doThrow(RemoveLastOrgAdminPermissionException.class).when(permissionDao).updatePermission(any());
      assertThrows(ConflictException.class, () -> permissionHandler.updatePermission(update));
    }

    @Test
    void workspacePermissionUpdatesDoNotModifyIdFields() throws Exception {
      final PermissionUpdate workspacePermissionUpdate = new PermissionUpdate()
          .permissionId(PERMISSION_WORKSPACE_READER.getPermissionId())
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_EDITOR); // changing to workspace_editor

      permissionHandler.updatePermission(workspacePermissionUpdate);

      verify(permissionDao).updatePermission(new Permission()
          .withPermissionId(PERMISSION_WORKSPACE_READER.getPermissionId())
          .withPermissionType(PermissionType.WORKSPACE_EDITOR)
          .withWorkspaceId(PERMISSION_WORKSPACE_READER.getWorkspaceId()) // workspace ID preserved from original permission
          .withUserId(PERMISSION_WORKSPACE_READER.getUserId())); // user ID preserved from original permission
    }

    @Test
    void organizationPermissionUpdatesDoNotModifyIdFields() throws Exception {
      final PermissionUpdate orgPermissionUpdate = new PermissionUpdate()
          .permissionId(PERMISSION_ORGANIZATION_ADMIN.getPermissionId())
          .permissionType(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_EDITOR); // changing to organization_editor

      permissionHandler.updatePermission(orgPermissionUpdate);

      verify(permissionDao).updatePermission(new Permission()
          .withPermissionId(PERMISSION_ORGANIZATION_ADMIN.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR)
          .withOrganizationId(PERMISSION_ORGANIZATION_ADMIN.getOrganizationId()) // organization ID preserved from original permission
          .withUserId(PERMISSION_ORGANIZATION_ADMIN.getUserId())); // user ID preserved from original permission
    }

  }

  @Nested
  class DeletePermission {

    private static final UUID ORGANIZATION_ID = UUID.randomUUID();

    private static final AuthenticatedUser USER = new AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withName("User")
        .withEmail("user@email.com");

    private static final Permission PERMISSION_WORKSPACE_READER = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(USER.getUserId())
        .withWorkspaceId(UUID.randomUUID())
        .withPermissionType(PermissionType.WORKSPACE_READER);

    private static final Permission PERMISSION_ORGANIZATION_ADMIN = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(USER.getUserId())
        .withOrganizationId(ORGANIZATION_ID)
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN);

    @Test
    void deletesPermission() throws Exception {
      when(permissionPersistence.getPermission(PERMISSION_WORKSPACE_READER.getPermissionId()))
          .thenReturn(Optional.of(PERMISSION_WORKSPACE_READER));

      permissionHandler.deletePermission(new PermissionIdRequestBody().permissionId(PERMISSION_WORKSPACE_READER.getPermissionId()));

      verify(permissionDao).deletePermission(PERMISSION_WORKSPACE_READER.getPermissionId());
    }

    @Test
    void throwsConflictIfPersistenceBlocks() throws Exception {
      doThrow(RemoveLastOrgAdminPermissionException.class).when(permissionDao).deletePermission(any());

      assertThrows(ConflictException.class, () -> permissionHandler.deletePermission(
          new PermissionIdRequestBody().permissionId(PERMISSION_ORGANIZATION_ADMIN.getPermissionId())));
    }

  }

  @Nested
  class CheckPermissions {

    private static final UUID WORKSPACE_ID = UUID.randomUUID();
    private static final UUID ORGANIZATION_ID = UUID.randomUUID();
    private static final UUID USER_ID = UUID.randomUUID();

    @Test
    void mismatchedUserId() throws IOException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.WORKSPACE_ADMIN)
          .withUserId(USER_ID)));

      final PermissionCheckRequest request = new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
          .userId(UUID.randomUUID()) // different user
          .workspaceId(WORKSPACE_ID);

      final PermissionCheckRead result = permissionHandler.checkPermissions(request);

      assertEquals(StatusEnum.FAILED, result.getStatus());
    }

    @Test
    void mismatchedWorkspaceId() throws IOException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(WORKSPACE_ID)
          .withUserId(USER_ID)));

      final PermissionCheckRequest request = new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
          .userId(USER_ID)
          .workspaceId(UUID.randomUUID()); // different workspace

      final PermissionCheckRead result = permissionHandler.checkPermissions(request);

      assertEquals(StatusEnum.FAILED, result.getStatus());
    }

    @Test
    void mismatchedOrganizationId() throws IOException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(ORGANIZATION_ID)
          .withUserId(USER_ID)));

      final PermissionCheckRequest request = new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN)
          .userId(USER_ID)
          .organizationId(UUID.randomUUID()); // different organization

      final PermissionCheckRead result = permissionHandler.checkPermissions(request);

      assertEquals(StatusEnum.FAILED, result.getStatus());
    }

    @Test
    void permissionsCheckMultipleWorkspaces() throws IOException {
      final UUID otherWorkspaceId = UUID.randomUUID();
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(
          new Permission()
              .withPermissionType(PermissionType.WORKSPACE_ADMIN)
              .withUserId(USER_ID)
              .withWorkspaceId(WORKSPACE_ID),
          new Permission()
              .withPermissionType(PermissionType.WORKSPACE_READER)
              .withUserId(USER_ID)
              .withWorkspaceId(otherWorkspaceId)));

      // EDITOR fails because READER is below editor
      final PermissionCheckRead editorResult = permissionHandler.permissionsCheckMultipleWorkspaces(new PermissionsCheckMultipleWorkspacesRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_EDITOR)
          .userId(USER_ID)
          .workspaceIds(List.of(WORKSPACE_ID, otherWorkspaceId)));

      assertEquals(StatusEnum.FAILED, editorResult.getStatus());

      // READER succeeds because both workspaces have at least READER permissions
      final PermissionCheckRead readerResult = permissionHandler.permissionsCheckMultipleWorkspaces(new PermissionsCheckMultipleWorkspacesRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_READER)
          .userId(USER_ID)
          .workspaceIds(List.of(WORKSPACE_ID, otherWorkspaceId)));

      assertEquals(StatusEnum.SUCCEEDED, readerResult.getStatus());
    }

    @Test
    void permissionsCheckMultipleWorkspacesOrgPermission() throws IOException, JsonValidationException, ConfigNotFoundException {
      final UUID otherWorkspaceId = UUID.randomUUID();
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(
          new Permission()
              .withPermissionType(PermissionType.WORKSPACE_ADMIN)
              .withUserId(USER_ID)
              .withWorkspaceId(WORKSPACE_ID),
          new Permission()
              .withPermissionType(PermissionType.ORGANIZATION_READER)
              .withUserId(USER_ID)
              .withOrganizationId(ORGANIZATION_ID)));

      // otherWorkspace is in the user's organization, so the user's Org Reader permission should apply
      when(workspaceService.getStandardWorkspaceNoSecrets(otherWorkspaceId, false))
          .thenReturn(new StandardWorkspace().withOrganizationId(ORGANIZATION_ID));

      // EDITOR fails because READER is below editor
      final PermissionCheckRead editorResult = permissionHandler.permissionsCheckMultipleWorkspaces(new PermissionsCheckMultipleWorkspacesRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_EDITOR)
          .userId(USER_ID)
          .workspaceIds(List.of(WORKSPACE_ID, otherWorkspaceId)));

      assertEquals(StatusEnum.FAILED, editorResult.getStatus());

      // READER succeeds because both workspaces have at least READER permissions
      final PermissionCheckRead readerResult = permissionHandler.permissionsCheckMultipleWorkspaces(new PermissionsCheckMultipleWorkspacesRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_READER)
          .userId(USER_ID)
          .workspaceIds(List.of(WORKSPACE_ID, otherWorkspaceId)));

      assertEquals(StatusEnum.SUCCEEDED, readerResult.getStatus());
    }

    @Test
    void workspaceNotInOrganization() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(ORGANIZATION_ID)
          .withUserId(USER_ID)));

      final StandardWorkspace workspace = mock(StandardWorkspace.class);
      when(workspace.getOrganizationId()).thenReturn(UUID.randomUUID()); // different organization
      when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)).thenReturn(workspace);

      final PermissionCheckRequest request = new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
          .userId(USER_ID)
          .workspaceId(WORKSPACE_ID);

      final PermissionCheckRead result = permissionHandler.checkPermissions(request);

      assertEquals(StatusEnum.FAILED, result.getStatus());
    }

    @ParameterizedTest
    @EnumSource(value = PermissionType.class,
                names = {"WORKSPACE_OWNER", "WORKSPACE_ADMIN", "WORKSPACE_EDITOR", "WORKSPACE_READER"})
    void workspaceLevelPermissions(final PermissionType userPermissionType) throws IOException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(userPermissionType)
          .withWorkspaceId(WORKSPACE_ID)
          .withUserId(USER_ID)));

      if (userPermissionType == PermissionType.WORKSPACE_OWNER) {
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }

      if (userPermissionType == PermissionType.WORKSPACE_ADMIN) {
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }

      if (userPermissionType == PermissionType.WORKSPACE_EDITOR) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }

      if (userPermissionType == PermissionType.WORKSPACE_READER) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }
    }

    @ParameterizedTest
    @EnumSource(value = PermissionType.class,
                names = {"ORGANIZATION_ADMIN", "ORGANIZATION_EDITOR", "ORGANIZATION_READER", "ORGANIZATION_MEMBER"})
    void organizationLevelPermissions(final PermissionType userPermissionType) throws IOException, JsonValidationException, ConfigNotFoundException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(userPermissionType)
          .withOrganizationId(ORGANIZATION_ID)
          .withUserId(USER_ID)));

      final StandardWorkspace workspace = mock(StandardWorkspace.class);
      when(workspace.getOrganizationId()).thenReturn(ORGANIZATION_ID);
      when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)).thenReturn(workspace);

      if (userPermissionType == PermissionType.ORGANIZATION_ADMIN) {
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }

      if (userPermissionType == PermissionType.ORGANIZATION_EDITOR) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }

      if (userPermissionType == PermissionType.ORGANIZATION_READER) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }

      if (userPermissionType == PermissionType.ORGANIZATION_MEMBER) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
        assertEquals(StatusEnum.FAILED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
      }
    }

    @Test
    void instanceAdminPermissions() throws IOException {
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.INSTANCE_ADMIN)
          .withUserId(USER_ID)));

      assertEquals(StatusEnum.SUCCEEDED, permissionHandler.checkPermissions(new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.INSTANCE_ADMIN)
          .userId(USER_ID)).getStatus());

      assertEquals(StatusEnum.SUCCEEDED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_ADMIN)).getStatus());
      assertEquals(StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_EDITOR)).getStatus());
      assertEquals(StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_READER)).getStatus());

      assertEquals(StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_ADMIN)).getStatus());
      assertEquals(StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_EDITOR)).getStatus());
      assertEquals(StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_READER)).getStatus());
      assertEquals(StatusEnum.SUCCEEDED,
          permissionHandler.checkPermissions(getOrganizationPermissionCheck(PermissionType.ORGANIZATION_MEMBER)).getStatus());
    }

    @Test
    void ensureAllPermissionTypesAreCovered() {
      final Set<PermissionType> coveredPermissionTypes = Set.of(
          PermissionType.INSTANCE_ADMIN,
          PermissionType.WORKSPACE_OWNER,
          PermissionType.WORKSPACE_ADMIN,
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_RUNNER,
          PermissionType.WORKSPACE_READER,
          PermissionType.ORGANIZATION_ADMIN,
          PermissionType.ORGANIZATION_EDITOR,
          PermissionType.ORGANIZATION_RUNNER,
          PermissionType.ORGANIZATION_READER,
          PermissionType.ORGANIZATION_MEMBER,
          PermissionType.DATAPLANE);

      // If this assertion fails, it means a new PermissionType was added! Please update either the
      // `organizationLevelPermissions` or `workspaceLeveLPermissions` tests above this one to
      // cover the new PermissionType. Once you've made sure that your new PermissionType is
      // covered, you can add it to the `coveredPermissionTypes` list above in order to make this
      // assertion pass.
      assertEquals(coveredPermissionTypes, Set.of(PermissionType.values()));
    }

    @Test
    void ensureNoExceptionOnOrgPermissionCheckForWorkspaceOutsideTheOrg() throws IOException, JsonValidationException, ConfigNotFoundException {
      // Ensure that when we check permissions for a workspace that's not in an organization against an
      // org permission, we don't throw an exception.
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(ORGANIZATION_ID)
          .withUserId(USER_ID),
          new Permission()
              .withPermissionType(PermissionType.WORKSPACE_ADMIN)
              .withWorkspaceId(WORKSPACE_ID)
              .withUserId(USER_ID)));

      when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false))
          .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID));

      assertEquals(StatusEnum.SUCCEEDED, permissionHandler.checkPermissions(new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
          .workspaceId(WORKSPACE_ID)
          .userId(USER_ID)).getStatus());

    }

    @Test
    void ensureFailedPermissionCheckForWorkspaceOutsideTheOrg() throws IOException, JsonValidationException, ConfigNotFoundException {
      // Ensure that when we check permissions for a workspace that's not in an organization against an
      // org permission, we fail the check if the workspace has no org ID set
      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(List.of(new Permission()
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(ORGANIZATION_ID)
          .withUserId(USER_ID),
          new Permission()
              .withPermissionType(PermissionType.WORKSPACE_ADMIN)
              .withWorkspaceId(WORKSPACE_ID)
              .withUserId(USER_ID)));

      when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false))
          .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID));

      assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(new PermissionCheckRequest()
          .permissionType(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN)
          .workspaceId(WORKSPACE_ID)
          .userId(USER_ID)).getStatus());

    }

    @Test
    void getPermissionsByServiceAccountIdReturnsPermissions() {
      final UUID serviceAccountId = UUID.randomUUID();
      final List<Permission> expected = List.of(new Permission()
          .withPermissionType(PermissionType.DATAPLANE)
          .withServiceAccountId(serviceAccountId));

      when(permissionDao.getPermissionsByServiceAccountId(eq(serviceAccountId))).thenReturn(expected);

      assertEquals(expected, permissionHandler.getPermissionsByServiceAccountId(serviceAccountId));
    }

    private PermissionCheckRequest getWorkspacePermissionCheck(final PermissionType targetPermissionType) {
      return new PermissionCheckRequest()
          .permissionType(Enums.convertTo(targetPermissionType, io.airbyte.api.model.generated.PermissionType.class))
          .userId(USER_ID)
          .workspaceId(WORKSPACE_ID);
    }

    private PermissionCheckRequest getOrganizationPermissionCheck(final PermissionType targetPermissionType) {
      return new PermissionCheckRequest()
          .permissionType(Enums.convertTo(targetPermissionType, io.airbyte.api.model.generated.PermissionType.class))
          .userId(USER_ID)
          .organizationId(ORGANIZATION_ID);
    }

  }

  @Nested
  class DeleteUserFromWorkspace {

    private static final UUID WORKSPACE_ID = UUID.randomUUID();
    private static final UUID USER_ID = UUID.randomUUID();

    @Test
    void testDeleteUserFromWorkspace() throws Exception {
      // should be deleted
      final Permission workspacePermission = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(USER_ID)
          .withWorkspaceId(WORKSPACE_ID)
          .withPermissionType(PermissionType.WORKSPACE_ADMIN);

      // should not be deleted, different workspace
      final Permission otherWorkspacePermission = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(USER_ID)
          .withWorkspaceId(UUID.randomUUID())
          .withPermissionType(PermissionType.WORKSPACE_ADMIN);

      // should not be deleted, org permission
      final Permission orgPermission = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(USER_ID)
          .withOrganizationId(UUID.randomUUID())
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN);

      when(permissionPersistence.listPermissionsByUser(USER_ID)).thenReturn(
          List.of(workspacePermission, otherWorkspacePermission, orgPermission));

      permissionHandler.deleteUserFromWorkspace(new PermissionDeleteUserFromWorkspaceRequestBody().userId(USER_ID).workspaceId(WORKSPACE_ID));

      // verify the intended permission was deleted
      verify(permissionDao).deletePermissions(List.of(workspacePermission.getPermissionId()));

      // verify the other permissions were not deleted
      verifyNoMoreInteractions(permissionDao);
    }

  }

}
