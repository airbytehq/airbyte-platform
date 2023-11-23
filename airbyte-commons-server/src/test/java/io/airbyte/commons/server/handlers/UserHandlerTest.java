/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.persistence.UserPersistence.DEFAULT_USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserRead;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserStatus;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUserRead;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
import io.airbyte.commons.auth.config.InitialUserConfiguration;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.support.JwtUserAuthenticationResolver;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;

@SuppressWarnings("PMD")
class UserHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private UserHandler userHandler;
  private UserPersistence userPersistence;
  private PermissionPersistence permissionPersistence;

  PermissionHandler permissionHandler;
  WorkspacesHandler workspacesHandler;
  OrganizationPersistence organizationPersistence;
  OrganizationsHandler organizationsHandler;
  JwtUserAuthenticationResolver jwtUserAuthenticationResolver;
  InitialUserConfiguration initialUserConfiguration;

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
    workspacesHandler = mock(WorkspacesHandler.class);
    organizationPersistence = mock(OrganizationPersistence.class);
    organizationsHandler = mock(OrganizationsHandler.class);
    uuidSupplier = mock(Supplier.class);
    jwtUserAuthenticationResolver = mock(JwtUserAuthenticationResolver.class);
    initialUserConfiguration = mock(InitialUserConfiguration.class);

    userHandler = new UserHandler(userPersistence, permissionPersistence, organizationPersistence, permissionHandler, workspacesHandler,
        uuidSupplier, Optional.of(jwtUserAuthenticationResolver), Optional.of(initialUserConfiguration));
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
    final UUID userID = UUID.randomUUID();

    // expecting the default user to be excluded from the response
    final UserPermission defaultUserPermission = new UserPermission()
        .withUser(new User().withName("default").withUserId(DEFAULT_USER_ID).withEmail("default@airbyte.io"))
        .withPermission(new Permission().withPermissionId(UUID.randomUUID()).withPermissionType(PermissionType.ORGANIZATION_ADMIN));

    final UserPermission realUserPermission = new UserPermission()
        .withUser(new User().withName(USER_NAME).withUserId(userID).withEmail(USER_EMAIL))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.ORGANIZATION_ADMIN));

    when(permissionPersistence.listUsersInOrganization(organizationId)).thenReturn(List.of(defaultUserPermission, realUserPermission));

    // no default user present
    var expectedListResult = new OrganizationUserReadList().users(List.of(new OrganizationUserRead()
        .name(USER_NAME)
        .userId(userID)
        .email(USER_EMAIL)
        .organizationId(organizationId)
        .permissionId(PERMISSION1_ID)
        .permissionType(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN)));

    final var result = userHandler.listUsersInOrganization(new OrganizationIdRequestBody().organizationId(organizationId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testListUsersInWorkspace() throws IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID userID = UUID.randomUUID();

    // expecting the default user to be excluded from the response
    final UserPermission defaultUserPermission = new UserPermission()
        .withUser(new User().withName("default").withUserId(DEFAULT_USER_ID).withEmail("default@airbyte.io"))
        .withPermission(new Permission().withPermissionId(UUID.randomUUID()).withPermissionType(PermissionType.WORKSPACE_ADMIN));

    final UserPermission realUserPermission = new UserPermission()
        .withUser(new User().withName(USER_NAME).withUserId(userID).withEmail(USER_EMAIL).withDefaultWorkspaceId(workspaceId))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.WORKSPACE_ADMIN));

    when(permissionPersistence.listUsersInWorkspace(workspaceId)).thenReturn(List.of(defaultUserPermission, realUserPermission));

    // no default user present
    var expectedListResult = new WorkspaceUserReadList().users(List.of(new WorkspaceUserRead()
        .userId(userID)
        .name(USER_NAME)
        .isDefaultWorkspace(true)
        .email(USER_EMAIL)
        .workspaceId(workspaceId)
        .permissionId(PERMISSION1_ID)
        .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)));

    final var result = userHandler.listUsersInWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
    assertEquals(expectedListResult, result);
  }

  @Test
  void testListInstanceAdminUser() throws Exception {
    when(permissionPersistence.listInstanceAdminUsers()).thenReturn(List.of(new UserPermission().withUser(
        new User().withName(USER_NAME).withUserId(USER_ID).withEmail(USER_EMAIL))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.INSTANCE_ADMIN))));

    final var result = userHandler.listInstanceAdminUsers();

    final var expectedResult = new UserWithPermissionInfoReadList().users(List.of(
        new io.airbyte.api.model.generated.UserWithPermissionInfoRead().name(USER_NAME).userId(USER_ID).email(USER_EMAIL)
            .permissionId(PERMISSION1_ID)));
    assertEquals(expectedResult, result);

  }

  @Nested
  class GetOrCreateUserByAuthIdTest {

    @ParameterizedTest
    @EnumSource(AuthProvider.class)
    void authIdExists(final AuthProvider authProvider) throws Exception {
      // set the auth provider for the existing user to match the test case
      user.setAuthProvider(authProvider);

      // authUserId is for the existing user
      final String authUserId = user.getAuthUserId();
      final io.airbyte.api.model.generated.AuthProvider apiAuthProvider =
          Enums.convertTo(authProvider, io.airbyte.api.model.generated.AuthProvider.class);

      when(userPersistence.getUserByAuthId(authUserId)).thenReturn(Optional.of(user));

      final UserRead userRead = userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(authUserId)).getUserRead();

      assertEquals(userRead.getUserId(), USER_ID);
      assertEquals(userRead.getEmail(), USER_EMAIL);
      assertEquals(userRead.getAuthUserId(), authUserId);
      assertEquals(userRead.getAuthProvider(), apiAuthProvider);
    }

    @Nested
    class NewUserTest {

      private static final String NEW_AUTH_USER_ID = "new_auth_user_id";
      private static final UUID NEW_USER_ID = UUID.randomUUID();
      private static final String NEW_EMAIL = "new@gmail.com";
      private static final String EXISTING_AUTH_USER_ID = "existing_auth_user_id";
      private static final UUID EXISTING_USER_ID = UUID.randomUUID();
      private static final String EXISTING_EMAIL = "existing@gmail.com";
      private static final UUID WORKSPACE_ID = UUID.randomUUID();

      private User newUser;
      private User existingUser;
      private WorkspaceRead defaultWorkspace;

      // this class provides the arguments for the parameterized test below, by returning all
      // permutations of auth provider, sso realm, initial user email, and deployment mode
      static class NewUserArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
          List<AuthProvider> authProviders = Arrays.asList(AuthProvider.values());
          List<String> ssoRealms = Arrays.asList("airbyte-realm", null);
          List<String> initialUserEmails = Arrays.asList(null, "", "other@gmail.com", NEW_EMAIL);
          List<Boolean> initialUserConfigPresent = Arrays.asList(true, false);
          List<Boolean> isFirstOrgUser = Arrays.asList(true, false);
          List<Boolean> isDefaultWorkspaceForOrgPresent = Arrays.asList(true, false);

          // return all permutations of the above input lists so that we can test all combinations.
          return authProviders.stream()
              .flatMap(
                  authProvider -> ssoRealms.stream()
                      .flatMap(
                          ssoRealm -> initialUserEmails.stream()
                              .flatMap(email -> initialUserConfigPresent.stream().flatMap(initialUserPresent -> isFirstOrgUser.stream()
                                  .flatMap(firstOrgUser -> isDefaultWorkspaceForOrgPresent.stream().flatMap(orgWorkspacePresent -> Stream
                                      .of(Arguments.of(authProvider, ssoRealm, email, initialUserPresent, firstOrgUser, orgWorkspacePresent))))))));
        }

      }

      @BeforeEach
      void setUp() throws IOException, JsonValidationException, ConfigNotFoundException {
        newUser = new User().withUserId(NEW_USER_ID).withEmail(NEW_EMAIL).withAuthUserId(NEW_AUTH_USER_ID);
        existingUser = new User().withUserId(EXISTING_USER_ID).withEmail(EXISTING_EMAIL).withAuthUserId(EXISTING_AUTH_USER_ID.toString());
        defaultWorkspace = new WorkspaceRead().workspaceId(WORKSPACE_ID);
        when(userPersistence.getUserByAuthId(anyString())).thenReturn(Optional.empty());
        when(jwtUserAuthenticationResolver.resolveUser(NEW_AUTH_USER_ID)).thenReturn(newUser);
        when(uuidSupplier.get()).thenReturn(NEW_USER_ID);
        when(userPersistence.getUser(NEW_USER_ID)).thenReturn(Optional.of(newUser));
        when(workspacesHandler.createDefaultWorkspaceForUser(any(), any())).thenReturn(defaultWorkspace);
      }

      @ParameterizedTest
      @ArgumentsSource(NewUserArgumentsProvider.class)
      void testNewUserCreation(final AuthProvider authProvider,
                               final String ssoRealm,
                               final String initialUserEmail,
                               final boolean initialUserPresent,
                               final boolean isFirstOrgUser,
                               final boolean isDefaultWorkspaceForOrgPresent)
          throws Exception {

        newUser.setAuthProvider(authProvider);

        when(jwtUserAuthenticationResolver.resolveSsoRealm()).thenReturn(ssoRealm);
        if (ssoRealm != null) {
          when(organizationPersistence.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(Optional.of(ORGANIZATION));
        }

        if (initialUserPresent) {
          if (initialUserEmail != null) {
            when(initialUserConfiguration.getEmail()).thenReturn(initialUserEmail);
          }
        } else {
          // replace default user handler with one that doesn't use initial user config (ie to test what
          // happens in Cloud)
          userHandler = new UserHandler(userPersistence, permissionPersistence, organizationPersistence, permissionHandler, workspacesHandler,
              uuidSupplier, Optional.of(jwtUserAuthenticationResolver), Optional.empty());
        }

        if (isFirstOrgUser) {
          when(permissionPersistence.listPermissionsForOrganization(ORGANIZATION.getOrganizationId())).thenReturn(List.of());
        } else {
          // add a pre-existing admin user for the org if this isn't the first user
          final var existingUserPermission = new UserPermission()
              .withUser(existingUser)
              .withPermission(new Permission().withPermissionType(PermissionType.ORGANIZATION_ADMIN));

          when(permissionPersistence.listPermissionsForOrganization(ORGANIZATION.getOrganizationId()))
              .thenReturn(List.of(existingUserPermission));
        }

        if (isDefaultWorkspaceForOrgPresent) {
          when(workspacesHandler.listWorkspacesInOrganization(
              new ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION.getOrganizationId()))).thenReturn(
                  new WorkspaceReadList().workspaces(List.of(defaultWorkspace)));
        } else {
          when(workspacesHandler.listWorkspacesInOrganization(any())).thenReturn(new WorkspaceReadList().workspaces(List.of()));
        }

        final io.airbyte.api.model.generated.AuthProvider apiAuthProvider =
            Enums.convertTo(authProvider, io.airbyte.api.model.generated.AuthProvider.class);

        final UserGetOrCreateByAuthIdResponse response = userHandler.getOrCreateUserByAuthId(
            new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID));
        final UserRead userRead = response.getUserRead();
        final boolean newUserCreated = response.getNewUserCreated();

        final InOrder userPersistenceInOrder = inOrder(userPersistence);

        assertTrue(newUserCreated);
        verifyCreatedUser(authProvider, userPersistenceInOrder);
        verifyUserRead(userRead, apiAuthProvider);
        verifyInstanceAdminPermissionCreation(initialUserEmail, initialUserPresent);
        verifyOrganizationPermissionCreation(ssoRealm, isFirstOrgUser);
        verifyDefaultWorkspaceCreation(ssoRealm, isDefaultWorkspaceForOrgPresent, userPersistenceInOrder);
      }

      @Test
      void testNewSsoUserWithoutOrgThrows() throws IOException {
        when(jwtUserAuthenticationResolver.resolveSsoRealm()).thenReturn("realm");
        when(organizationPersistence.getOrganizationBySsoConfigRealm("realm")).thenReturn(Optional.empty());
        assertThrows(ConfigNotFoundException.class, () -> userHandler.getOrCreateUserByAuthId(
            new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID)));
      }

      private void verifyCreatedUser(final AuthProvider expectedAuthProvider, final InOrder inOrder) throws IOException {
        inOrder.verify(userPersistence).writeUser(argThat(user -> user.getUserId().equals(NEW_USER_ID)
            && NEW_EMAIL.equals(user.getEmail())
            && NEW_AUTH_USER_ID.equals(user.getAuthUserId())
            && user.getAuthProvider().equals(expectedAuthProvider)));
      }

      private void verifyDefaultWorkspaceCreation(final String ssoRealm, final Boolean isDefaultWorkspaceForOrgPresent, final InOrder inOrder)
          throws IOException, JsonValidationException, ConfigNotFoundException {
        boolean workspaceCreated = false;

        if (ssoRealm == null) {
          // always create a default workspace for non-SSO users
          verify(workspacesHandler).createDefaultWorkspaceForUser(
              argThat(user -> user.getUserId().equals(NEW_USER_ID)),
              eq(Optional.empty()));
          workspaceCreated = true;

        } else {
          if (!isDefaultWorkspaceForOrgPresent) {
            // create a default workspace for the org if one doesn't yet exist
            verify(workspacesHandler).createDefaultWorkspaceForUser(
                argThat(user -> user.getUserId().equals(NEW_USER_ID)),
                argThat(org -> org.orElseThrow().getOrganizationId().equals(ORGANIZATION.getOrganizationId())));
            workspaceCreated = true;

          } else {
            // never create an additional workspace for the org if one already exists.
            verify(workspacesHandler, never()).createDefaultWorkspaceForUser(any(), any());
          }
        }
        if (workspaceCreated) {
          // if a workspace was created, verify that the user's defaultWorkspaceId was updated
          // and that a workspaceAdmin permission was created for them.
          inOrder.verify(userPersistence).writeUser(argThat(user -> user.getDefaultWorkspaceId().equals(WORKSPACE_ID)));
          verify(permissionHandler).createPermission(new PermissionCreate()
              .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
              .workspaceId(WORKSPACE_ID)
              .userId(NEW_USER_ID));
        }
      }

      private void verifyUserRead(final UserRead userRead, final io.airbyte.api.model.generated.AuthProvider expectedAuthProvider) {
        assertEquals(userRead.getUserId(), NEW_USER_ID);
        assertEquals(userRead.getEmail(), NEW_EMAIL);
        assertEquals(userRead.getAuthUserId(), NEW_AUTH_USER_ID);
        assertEquals(userRead.getAuthProvider(), expectedAuthProvider);
      }

      private void verifyInstanceAdminPermissionCreation(final String initialUserEmail, final boolean initialUserPresent)
          throws IOException {
        // instance_admin permissions should only ever be created when the initial user config is present
        // (which should never be true in Cloud).
        // also, if the initial user email is null or doesn't match the new user's email, no instance_admin
        // permission should be created
        if (!initialUserPresent || initialUserEmail == null || !initialUserEmail.equalsIgnoreCase(NEW_EMAIL)) {
          verify(permissionPersistence, never())
              .writePermission(argThat(permission -> permission.getPermissionType().equals(PermissionType.INSTANCE_ADMIN)));
        } else {
          // otherwise, instance_admin permission should be created
          verify(permissionPersistence).writePermission(argThat(
              permission -> permission.getPermissionType().equals(PermissionType.INSTANCE_ADMIN) && permission.getUserId().equals(NEW_USER_ID)));
        }
      }

      private void verifyOrganizationPermissionCreation(final String ssoRealm, final boolean isFirstOrgUser)
          throws IOException, JsonValidationException {
        // if the SSO Realm is null, no organization permission should be created
        if (ssoRealm == null) {
          verify(permissionHandler, never()).createPermission(
              argThat(permission -> permission.getPermissionType().equals(io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN)));
        } else {
          final io.airbyte.api.model.generated.PermissionType expectedPermissionType = isFirstOrgUser
              ? io.airbyte.api.model.generated.PermissionType.ORGANIZATION_ADMIN
              : io.airbyte.api.model.generated.PermissionType.ORGANIZATION_MEMBER;
          // otherwise, organization permission should be created for the associated user and org.
          verify(permissionHandler).createPermission(new PermissionCreate()
              .permissionType(expectedPermissionType)
              .organizationId(ORGANIZATION.getOrganizationId())
              .userId(NEW_USER_ID));
        }
      }

    }

  }

}
