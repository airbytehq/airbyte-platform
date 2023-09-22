/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.User.AuthProvider.GOOGLE_IDENTITY_PLATFORM;
import static io.airbyte.config.User.AuthProvider.KEYCLOAK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationUserRead;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserStatus;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceUserRead;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
import io.airbyte.commons.server.support.JwtUserResolver;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.User.Status;
import io.airbyte.config.UserPermission;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UserHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private UserHandler userHandler;
  private UserPersistence userPersistence;
  private PermissionPersistence permissionPersistence;

  PermissionHandler permissionHandler;
  OrganizationPersistence organizationPersistence;
  OrganizationsHandler organizationsHandler;
  JwtUserResolver jwtUserResolver;

  private static final UUID USER_ID = UUID.randomUUID();
  private static final String USER_NAME = "user 1";
  private static final String USER_EMAIL = "user_1@whatever.com";

  private static final Organization ORGANIZATION = new Organization().withOrganizationId(UUID.randomUUID()).withName(USER_NAME).withEmail(USER_EMAIL);
  private static final UUID PERMISSION1_ID = UUID.randomUUID();

  private final User user = new User()
      .withUserId(USER_ID)
      .withAuthUserId(USER_ID.toString())
      .withEmail(USER_EMAIL)
      .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
      .withStatus(Status.INVITED)
      .withName(USER_NAME);

  @BeforeEach
  void setUp() {
    userPersistence = mock(UserPersistence.class);
    permissionPersistence = mock(PermissionPersistence.class);
    permissionHandler = mock(PermissionHandler.class);
    organizationPersistence = mock(OrganizationPersistence.class);
    organizationsHandler = mock(OrganizationsHandler.class);
    uuidSupplier = mock(Supplier.class);
    jwtUserResolver = mock(JwtUserResolver.class);

    userHandler = new UserHandler(userPersistence, permissionPersistence, organizationPersistence, permissionHandler, organizationsHandler,
        uuidSupplier, Optional.of(jwtUserResolver));
  }

  @Test
  void testCreateUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(uuidSupplier.get()).thenReturn(USER_ID);
    when(userPersistence.getUser(any())).thenReturn(Optional.of(user));
    final UserCreate userCreate = new UserCreate()
        .name(USER_NAME)
        .authUserId(USER_ID.toString())
        .authProvider(
            io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .status(UserStatus.DISABLED.INVITED)
        .email(USER_EMAIL);
    final UserRead actualRead = userHandler.createUser(userCreate);
    final UserRead expectedRead = new UserRead()
        .userId(USER_ID)
        .name(USER_NAME)
        .authUserId(USER_ID.toString())
        .authProvider(
            io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .status(UserStatus.DISABLED.INVITED)
        .email(USER_EMAIL)
        .companyName(null)
        .metadata(null)
        .news(false);

    assertEquals(expectedRead, actualRead);
  }

  @Test
  void testListUsersInOrg() throws Exception {
    final UUID organizationId = UUID.randomUUID();
    final UUID USER_ID = UUID.randomUUID();

    when(permissionPersistence.listUsersInOrganization(organizationId)).thenReturn(List.of(new UserPermission().withUser(
        new User().withName(USER_NAME).withUserId(USER_ID).withEmail(USER_EMAIL))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.ORGANIZATION_ADMIN))));

    var expectedListResult =
        new OrganizationUserReadList()
            .users(List.of(new OrganizationUserRead().name(USER_NAME).userId(USER_ID).email(USER_EMAIL).organizationId(organizationId)
                .permissionId(PERMISSION1_ID).permissionType(
                    io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN)));

    var result = userHandler.listUsersInOrganization(new OrganizationIdRequestBody().organizationId(organizationId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testListUsersInWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID USER_ID = UUID.randomUUID();

    when(permissionPersistence.listUsersInWorkspace(workspaceId)).thenReturn(List.of(new UserPermission().withUser(
        new User().withUserId(USER_ID).withEmail(USER_EMAIL).withName(USER_NAME).withDefaultWorkspaceId(workspaceId))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.WORKSPACE_ADMIN))));

    var expectedListResult =
        new WorkspaceUserReadList().users(List.of(
            new WorkspaceUserRead().userId(USER_ID).name(USER_NAME).isDefaultWorkspace(true).email(USER_EMAIL).workspaceId(workspaceId)
                .permissionId(PERMISSION1_ID)
                .permissionType(
                    io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)));

    var result = userHandler.listUsersInWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testListInstanceAdminUser() throws Exception {
    when(permissionPersistence.listInstanceAdminUsers()).thenReturn(List.of(new UserPermission().withUser(
        new User().withName(USER_NAME).withUserId(USER_ID).withEmail(USER_EMAIL))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.INSTANCE_ADMIN))));

    var result = userHandler.listInstanceAdminUsers();

    var expectedResult = new UserWithPermissionInfoReadList().users(List.of(
        new io.airbyte.api.model.generated.UserWithPermissionInfoRead().name(USER_NAME).userId(USER_ID).email(USER_EMAIL)
            .permissionId(PERMISSION1_ID)));
    assertEquals(expectedResult, result);

  }

  @Nested
  class GetOrCreateUserByAuthIdTest {

    private static final String SSO_REALM = "airbyte-realm";

    private static final String KEY_CLOAK_AUTH_ID = "key_cloack_auth_id";
    private static final String GOOGLE_AUTH_ID = "google_auth_id";

    private static final User RESOLVED_USER_TEMPLATE = new User().withEmail(USER_EMAIL).withName(USER_NAME);

    @Test
    void testGetOrCreateUserByAuthId_authIdExists() throws Exception {
      when(userPersistence.getUserByAuthId(KEY_CLOAK_AUTH_ID, KEYCLOAK)).thenReturn(Optional.of(user));
      UserRead userRead = userHandler.getOrCreateUserByAuthId(
          new UserAuthIdRequestBody().authProvider(io.airbyte.api.model.generated.AuthProvider.KEYCLOAK).authUserId(KEY_CLOAK_AUTH_ID));

      assertEquals(userRead.getUserId(), user.getUserId());
    }

    @Test
    void testGetOrCreateUserByAuthId_firebaseUser_authIdNotExists() throws Exception {
      when(userPersistence.getUserByAuthId(GOOGLE_AUTH_ID, GOOGLE_IDENTITY_PLATFORM)).thenReturn(Optional.empty());

      final User resolvedUser =
          RESOLVED_USER_TEMPLATE.withAuthUserId(GOOGLE_AUTH_ID).withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM);

      when(jwtUserResolver.resolveUser()).thenReturn(resolvedUser);
      when(jwtUserResolver.resolveSsoRealm()).thenReturn(null);
      when(uuidSupplier.get()).thenReturn(USER_ID);
      when(userPersistence.getUser(USER_ID)).thenReturn(Optional.of(user));

      when(organizationsHandler.createOrganization(any())).thenReturn(new OrganizationRead().organizationId(ORGANIZATION.getOrganizationId()));

      final UserRead userRead = userHandler.getOrCreateUserByAuthId(
          new UserAuthIdRequestBody().authProvider(io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM).authUserId(GOOGLE_AUTH_ID));

      assertEquals(userRead.getUserId(), USER_ID);
      assertEquals(userRead.getEmail(), USER_EMAIL);

      verify(userPersistence).writeUser(any());

      verify(organizationsHandler, never()).createOrganization(any());
      verify(permissionHandler, never()).createPermission(any());
    }

    @Test
    void testGetOrCreateUserByAuthId_SsoUser_authIdNotExists() throws Exception {
      when(userPersistence.getUserByAuthId(KEY_CLOAK_AUTH_ID, KEYCLOAK)).thenReturn(Optional.empty());

      final User resolvedUser =
          RESOLVED_USER_TEMPLATE.withAuthUserId(KEY_CLOAK_AUTH_ID).withAuthProvider(AuthProvider.KEYCLOAK);

      when(jwtUserResolver.resolveUser()).thenReturn(resolvedUser);
      when(jwtUserResolver.resolveSsoRealm()).thenReturn(SSO_REALM);
      when(organizationPersistence.getOrganizationBySsoConfigRealm(SSO_REALM)).thenReturn(Optional.of(ORGANIZATION));
      when(uuidSupplier.get()).thenReturn(USER_ID);

      when(userPersistence.getUser(USER_ID)).thenReturn(Optional.of(user));

      UserRead userRead = userHandler.getOrCreateUserByAuthId(
          new UserAuthIdRequestBody().authProvider(io.airbyte.api.model.generated.AuthProvider.KEYCLOAK).authUserId(KEY_CLOAK_AUTH_ID));

      verify(userPersistence).writeUser(any());

      verify(permissionHandler).createPermission(new PermissionCreate()
          .permissionType(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN).organizationId(ORGANIZATION.getOrganizationId())
          .userId(USER_ID));

      assertEquals(userRead.getUserId(), USER_ID);
      assertEquals(userRead.getEmail(), USER_EMAIL);
    }

    @Test
    void testGetOrCreateUserByAuthId_SsoUser_noAuthIdNorOrg() throws Exception {
      when(userPersistence.getUserByAuthId(KEY_CLOAK_AUTH_ID, KEYCLOAK)).thenReturn(Optional.empty());

      final User resolvedUser =
          RESOLVED_USER_TEMPLATE.withAuthUserId(KEY_CLOAK_AUTH_ID).withAuthProvider(AuthProvider.KEYCLOAK);

      when(jwtUserResolver.resolveUser()).thenReturn(resolvedUser);
      when(jwtUserResolver.resolveSsoRealm()).thenReturn(SSO_REALM);
      when(organizationPersistence.getOrganizationBySsoConfigRealm(SSO_REALM)).thenReturn(Optional.empty());
      when(uuidSupplier.get()).thenReturn(USER_ID);
      when(userPersistence.getUser(USER_ID)).thenReturn(Optional.of(user));
      when(organizationsHandler.createOrganization(any())).thenReturn(new OrganizationRead().organizationId(ORGANIZATION.getOrganizationId()));

      final UserRead userRead = userHandler.getOrCreateUserByAuthId(
          new UserAuthIdRequestBody().authProvider(io.airbyte.api.model.generated.AuthProvider.KEYCLOAK).authUserId(KEY_CLOAK_AUTH_ID));

      verify(userPersistence).writeUser(any());
      assertEquals(userRead.getUserId(), USER_ID);
      assertEquals(userRead.getEmail(), USER_EMAIL);

      verify(organizationsHandler, never()).createOrganization(any());
      verify(permissionHandler, never()).createPermission(any());
    }

  }

}
