/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PermissionHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private PermissionHandler permissionHandler;
  private PermissionPersistence permissionPersistence;
  private WorkspaceService workspaceService;

  private final UUID userId = UUID.randomUUID();
  private final UUID workspaceId = UUID.randomUUID();
  private final UUID permissionId = UUID.randomUUID();
  private final Permission permission = new Permission()
      .withPermissionId(permissionId)
      .withUserId(userId)
      .withWorkspaceId(workspaceId)
      .withPermissionType(PermissionType.WORKSPACE_ADMIN);

  @BeforeEach
  void setUp() {
    permissionPersistence = mock(PermissionPersistence.class);
    uuidSupplier = mock(Supplier.class);
    workspaceService = mock(WorkspaceService.class);
    permissionHandler = new PermissionHandler(permissionPersistence, workspaceService, uuidSupplier);
  }

  @Test
  void testCreatePermission() throws IOException {
    final List<Permission> existingPermissions = List.of();
    when(permissionPersistence.listPermissionsByUser(any())).thenReturn(existingPermissions);
    when(uuidSupplier.get()).thenReturn(permissionId);
    when(permissionPersistence.getPermission(any())).thenReturn(Optional.of(permission));
    final PermissionCreate permissionCreate = new PermissionCreate()
        .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_OWNER)
        .userId(userId)
        .workspaceId(workspaceId);
    final PermissionRead actualRead = permissionHandler.createPermission(permissionCreate);
    final PermissionRead expectedRead = new PermissionRead()
        .permissionId(permissionId)
        .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
        .userId(userId)
        .workspaceId(workspaceId);

    assertEquals(expectedRead, actualRead);
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
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
      }

      if (userPermissionType == PermissionType.WORKSPACE_ADMIN) {
        assertEquals(StatusEnum.SUCCEEDED,
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
      }

      if (userPermissionType == PermissionType.WORKSPACE_EDITOR) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
      }

      if (userPermissionType == PermissionType.WORKSPACE_READER) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
      }
    }

    @ParameterizedTest
    @EnumSource(value = PermissionType.class,
                names = {"ORGANIZATION_ADMIN", "ORGANIZATION_EDITOR", "ORGANIZATION_READER"})
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
            permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
      }

      if (userPermissionType == PermissionType.ORGANIZATION_EDITOR) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
      }

      if (userPermissionType == PermissionType.ORGANIZATION_READER) {
        assertEquals(StatusEnum.FAILED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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

      assertEquals(StatusEnum.SUCCEEDED, permissionHandler.checkPermissions(getWorkspacePermissionCheck(PermissionType.WORKSPACE_OWNER)).getStatus());
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
    }

    @Test
    void ensureAllPermissionTypesAreCovered() {
      final Set<PermissionType> coveredPermissionTypes = Set.of(
          PermissionType.INSTANCE_ADMIN,
          PermissionType.WORKSPACE_OWNER,
          PermissionType.WORKSPACE_ADMIN,
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_READER,
          PermissionType.ORGANIZATION_ADMIN,
          PermissionType.ORGANIZATION_EDITOR,
          PermissionType.ORGANIZATION_READER);

      // If this assertion fails, it means a new PermissionType was added! Please update either the
      // `organizationLevelPermissions` or `workspaceLeveLPermissions` tests above this one to
      // cover the new PermissionType. Once you've made sure that your new PermissionType is
      // covered, you can add it to the `coveredPermissionTypes` list above in order to make this
      // assertion pass.
      assertEquals(coveredPermissionTypes, Set.of(PermissionType.values()));
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

}
