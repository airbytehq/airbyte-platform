/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserRead;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserStatus;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceUserRead;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.User.Status;
import io.airbyte.config.UserPermission;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private UserHandler userHandler;
  private UserPersistence userPersistence;
  private PermissionPersistence permissionPersistence;

  private final UUID userId = UUID.randomUUID();
  private final String userName = "user 1";
  private final String userEmail = "user_1@whatever.com";
  private final UUID permission1Id = UUID.randomUUID();
  private final UUID permission2Id = UUID.randomUUID();

  private final User user = new User()
      .withUserId(userId)
      .withAuthUserId(userId.toString())
      .withEmail(userEmail)
      .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
      .withStatus(Status.INVITED)
      .withName(userName);

  @BeforeEach
  void setUp() {
    userPersistence = mock(UserPersistence.class);
    permissionPersistence = mock(PermissionPersistence.class);
    uuidSupplier = mock(Supplier.class);
    userHandler = new UserHandler(userPersistence, permissionPersistence, uuidSupplier);
  }

  @Test
  void testCreateUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(uuidSupplier.get()).thenReturn(userId);
    when(userPersistence.getUser(any())).thenReturn(Optional.of(user));
    final UserCreate userCreate = new UserCreate()
        .name(userName)
        .authUserId(userId.toString())
        .authProvider(
            io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .status(UserStatus.DISABLED.INVITED)
        .email(userEmail);
    final UserRead actualRead = userHandler.createUser(userCreate);
    final UserRead expectedRead = new UserRead()
        .userId(userId)
        .name(userName)
        .authUserId(userId.toString())
        .authProvider(
            io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .status(UserStatus.DISABLED.INVITED)
        .email(userEmail)
        .companyName(null)
        .metadata(null)
        .news(false);

    assertEquals(expectedRead, actualRead);
  }

  @Test
  void testListUsersInOrg() throws Exception {
    final UUID organizationId = UUID.randomUUID();
    final UUID userId = UUID.randomUUID();

    when(permissionPersistence.listUsersInOrganization(organizationId)).thenReturn(List.of(new UserPermission().withUser(
        new User().withName(userName).withUserId(userId).withEmail(userEmail))
        .withPermission(new Permission().withPermissionId(permission1Id).withPermissionType(PermissionType.ORGANIZATION_ADMIN))));

    var expectedListResult =
        new OrganizationUserReadList()
            .users(List.of(new OrganizationUserRead().name(userName).userId(userId).email(userEmail).organizationId(organizationId)
                .permissionId(permission1Id).permissionType(
                    io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN)));

    var result = userHandler.listUsersInOrganization(new OrganizationIdRequestBody().organizationId(organizationId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testMergeUserPermissionsInOrg() throws Exception {
    final UUID organizationId = UUID.randomUUID();
    final UUID userId = UUID.randomUUID();

    when(permissionPersistence.listUsersInOrganization(organizationId)).thenReturn(List.of(
        new UserPermission()
            .withUser(new User().withName(userName).withUserId(userId).withEmail(userEmail))
            .withPermission(new Permission().withPermissionId(permission1Id).withPermissionType(PermissionType.ORGANIZATION_ADMIN)),
        new UserPermission()
            .withUser(new User().withName(userName).withUserId(userId).withEmail(userEmail))
            .withPermission(new Permission().withPermissionId(permission2Id).withPermissionType(PermissionType.INSTANCE_ADMIN))));

    var expectedListResult =
        new OrganizationUserReadList()
            .users(List.of(new OrganizationUserRead().name(userName).userId(userId).email(userEmail).organizationId(organizationId)
                .permissionId(permission2Id)
                .permissionType(
                    io.airbyte.api.model.generated.PermissionType.INSTANCE_ADMIN)));

    var result = userHandler.listUsersInOrganization(new OrganizationIdRequestBody().organizationId(organizationId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testListUsersInWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID userId = UUID.randomUUID();

    when(permissionPersistence.listUsersInWorkspace(workspaceId)).thenReturn(List.of(new UserPermission().withUser(
        new User().withUserId(userId).withEmail(userEmail).withName(userName).withDefaultWorkspaceId(workspaceId))
        .withPermission(new Permission().withPermissionId(permission1Id).withPermissionType(PermissionType.WORKSPACE_ADMIN))));

    var expectedListResult =
        new WorkspaceUserReadList().users(List.of(
            new WorkspaceUserRead().userId(userId).name(userName).isDefaultWorkspace(true).email(userEmail).workspaceId(workspaceId)
                .permissionId(permission1Id)
                .permissionType(
                    io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)));

    var result = userHandler.listUsersInWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testListUsersWithMultiplePermissionInWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID userId = UUID.randomUUID();

    when(permissionPersistence.listUsersInWorkspace(workspaceId)).thenReturn(List.of(
        new UserPermission()
            .withUser(new User().withUserId(userId).withEmail(userEmail).withName(userName).withDefaultWorkspaceId(workspaceId))
            .withPermission(new Permission().withPermissionId(permission1Id).withPermissionType(PermissionType.WORKSPACE_ADMIN)),
        new UserPermission()
            .withUser(new User().withUserId(userId).withEmail(userEmail).withName(userName).withDefaultWorkspaceId(workspaceId))
            .withPermission(new Permission().withPermissionId(permission2Id).withPermissionType(PermissionType.INSTANCE_ADMIN))));

    var expectedListResult =
        new WorkspaceUserReadList().users(List.of(
            new WorkspaceUserRead()
                .name(userName)
                .isDefaultWorkspace(true)
                .userId(userId)
                .email(userEmail)
                .workspaceId(workspaceId)
                .permissionId(permission2Id)
                .permissionType(
                    io.airbyte.api.model.generated.PermissionType.INSTANCE_ADMIN)));

    var result = userHandler.listUsersInWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
    assertEquals(expectedListResult, result);
  }

}
