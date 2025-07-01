/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.ConstantsKt.DEFAULT_USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList;
import io.airbyte.api.problems.throwable.generated.SSORequiredProblem;
import io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem;
import io.airbyte.commons.auth.config.InitialUserConfig;
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Application;
import io.airbyte.config.AuthProvider;
import io.airbyte.config.AuthUser;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.Organization;
import io.airbyte.config.OrganizationEmailDomain;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.SsoConfig;
import io.airbyte.config.User;
import io.airbyte.config.User.Status;
import io.airbyte.config.UserPermission;
import io.airbyte.config.WorkspaceUserAccessInfo;
import io.airbyte.config.helpers.AuthenticatedUserConverter;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.services.ApplicationService;
import io.airbyte.data.services.ExternalUserService;
import io.airbyte.data.services.OrganizationEmailDomainService;
import io.airbyte.data.services.PermissionRedundantException;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RestrictLoginsForSSODomains;
import io.airbyte.featureflag.TestClient;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

@SuppressWarnings("PMD")
class UserHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private UserHandler userHandler;
  private UserPersistence userPersistence;

  PermissionHandler permissionHandler;
  WorkspacesHandler workspacesHandler;
  OrganizationPersistence organizationPersistence;
  OrganizationEmailDomainService organizationEmailDomainService;
  OrganizationsHandler organizationsHandler;
  JwtUserAuthenticationResolver jwtUserAuthenticationResolver;
  InitialUserConfig initialUserConfig;
  ExternalUserService externalUserService;
  ApplicationService applicationService;
  FeatureFlagClient featureFlagClient;

  private static final UUID USER_ID = UUID.randomUUID();
  private static final String USER_NAME = "user 1";
  private static final String USER_EMAIL = "user_1@whatever.com";

  private static final Organization ORGANIZATION = new Organization().withOrganizationId(UUID.randomUUID()).withName(USER_NAME).withEmail(USER_EMAIL);
  private static final UUID PERMISSION1_ID = UUID.randomUUID();

  private final AuthenticatedUser user = new AuthenticatedUser()
      .withUserId(USER_ID)
      .withAuthUserId(USER_ID.toString())
      .withEmail(USER_EMAIL)
      .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
      .withStatus(Status.INVITED)
      .withName(USER_NAME);
  private ResourceBootstrapHandler resourceBootstrapHandler;

  @BeforeEach
  void setUp() {
    userPersistence = mock(UserPersistence.class);
    permissionHandler = mock(PermissionHandler.class);
    workspacesHandler = mock(WorkspacesHandler.class);
    organizationPersistence = mock(OrganizationPersistence.class);
    organizationEmailDomainService = mock(OrganizationEmailDomainService.class);
    organizationsHandler = mock(OrganizationsHandler.class);
    uuidSupplier = mock(Supplier.class);
    jwtUserAuthenticationResolver = mock(JwtUserAuthenticationResolver.class);
    initialUserConfig = mock(InitialUserConfig.class);
    resourceBootstrapHandler = mock(ResourceBootstrapHandler.class);
    externalUserService = mock(ExternalUserService.class);
    applicationService = mock(ApplicationService.class);
    featureFlagClient = mock(TestClient.class);

    when(featureFlagClient.boolVariation(eq(RestrictLoginsForSSODomains.INSTANCE), any())).thenReturn(true);

    userHandler =
        new UserHandler(userPersistence, externalUserService, organizationPersistence,
            organizationEmailDomainService, Optional.of(applicationService),
            permissionHandler, workspacesHandler,
            uuidSupplier, jwtUserAuthenticationResolver, Optional.of(initialUserConfig), resourceBootstrapHandler, featureFlagClient);
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

    when(permissionHandler.listUsersInOrganization(organizationId)).thenReturn(List.of(defaultUserPermission, realUserPermission));

    // no default user present
    final var expectedListResult = new OrganizationUserReadList().users(List.of(new OrganizationUserRead()
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
  void testListInstanceAdminUser() throws Exception {
    when(permissionHandler.listInstanceAdminUsers()).thenReturn(List.of(new UserPermission().withUser(
        new User().withName(USER_NAME).withUserId(USER_ID).withEmail(USER_EMAIL))
        .withPermission(new Permission().withPermissionId(PERMISSION1_ID).withPermissionType(PermissionType.INSTANCE_ADMIN))));

    final var result = userHandler.listInstanceAdminUsers();

    final var expectedResult = new UserWithPermissionInfoReadList().users(List.of(
        new io.airbyte.api.model.generated.UserWithPermissionInfoRead().name(USER_NAME).userId(USER_ID).email(USER_EMAIL)
            .permissionId(PERMISSION1_ID)));
    assertEquals(expectedResult, result);

  }

  @Test
  void testListAccessInfoByWorkspaceId() throws Exception {
    final UUID workspaceId = UUID.randomUUID();
    when(userPersistence.listWorkspaceUserAccessInfo(workspaceId)).thenReturn(List.of(
        new WorkspaceUserAccessInfo()
            .withUserId(DEFAULT_USER_ID), // expect the default user to be filtered out.
        new WorkspaceUserAccessInfo()
            .withUserId(USER_ID)
            .withUserName(USER_NAME)
            .withUserEmail(USER_EMAIL)
            .withWorkspaceId(workspaceId)
            .withWorkspacePermission(new Permission()
                .withPermissionId(PERMISSION1_ID)
                .withPermissionType(PermissionType.WORKSPACE_ADMIN)
                .withUserId(USER_ID)
                .withWorkspaceId(workspaceId))));

    final var result = userHandler.listAccessInfoByWorkspaceId(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    final var expected = new WorkspaceUserAccessInfoReadList().usersWithAccess(List.of(
        new io.airbyte.api.model.generated.WorkspaceUserAccessInfoRead()
            .userId(USER_ID)
            .userName(USER_NAME)
            .userEmail(USER_EMAIL)
            .workspaceId(workspaceId)
            .workspacePermission(new PermissionRead()
                .permissionId(PERMISSION1_ID)
                .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN)
                .userId(USER_ID)
                .workspaceId(workspaceId))));

    assertEquals(expected, result);
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

      when(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(user);
      when(userPersistence.getUserByAuthId(authUserId)).thenReturn(Optional.of(user));

      final UserGetOrCreateByAuthIdResponse response = userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(authUserId));
      final UserRead userRead = response.getUserRead();

      assertEquals(userRead.getUserId(), USER_ID);
      assertEquals(userRead.getEmail(), USER_EMAIL);
      assertEquals(response.getAuthUserId(), authUserId);
      assertEquals(response.getAuthProvider(), apiAuthProvider);
    }

    @Nested
    class ExistingEmailTest {

      private static final UUID EXISTING_USER_ID = UUID.randomUUID();
      private static final String EXISTING_AUTH_USER_ID = "existing_auth_user_id";
      private static final String NEW_AUTH_USER_ID = "new_auth_user_id";
      private static final String EMAIL = "user@airbyte.io";
      private static final String SSO_REALM = "airbyte-realm";
      private static final String REALM = "_airbyte-users";

      private AuthenticatedUser jwtUser;
      private User existingUser;

      @BeforeEach
      void setup() {
        jwtUser = new AuthenticatedUser().withEmail(EMAIL).withAuthUserId(NEW_AUTH_USER_ID).withAuthProvider(AuthProvider.KEYCLOAK);
        existingUser = new User().withUserId(EXISTING_USER_ID).withEmail(EMAIL);
      }

      @ParameterizedTest
      @CsvSource({"true", "false"})
      void testNonSSOSignInEmailExistsThrowsError(final Boolean isExistingUserSSO) throws Exception {
        when(jwtUserAuthenticationResolver.resolveUser(NEW_AUTH_USER_ID)).thenReturn(jwtUser);
        when(userPersistence.getUserByAuthId(NEW_AUTH_USER_ID)).thenReturn(Optional.empty());
        when(userPersistence.getUserByEmail(EMAIL)).thenReturn(Optional.of(existingUser));
        when(userPersistence.listAuthUsersForUser(EXISTING_USER_ID))
            .thenReturn(List.of(new AuthUser().withAuthUserId(EXISTING_AUTH_USER_ID).withAuthProvider(AuthProvider.KEYCLOAK)));
        when(externalUserService.getRealmByAuthUserId(EXISTING_AUTH_USER_ID)).thenReturn(REALM);

        if (isExistingUserSSO) {
          when(organizationPersistence.getSsoConfigByRealmName(REALM)).thenReturn(Optional.of(new SsoConfig()));
        }

        assertThrows(UserAlreadyExistsProblem.class,
            () -> userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID)));
      }

      @Test
      void testExistingDefaultUserWithEmailUpdatesDefault()
          throws IOException, JsonValidationException, ConfigNotFoundException, PermissionRedundantException {
        when(jwtUserAuthenticationResolver.resolveUser(NEW_AUTH_USER_ID)).thenReturn(jwtUser);
        when(userPersistence.getUserByAuthId(NEW_AUTH_USER_ID)).thenReturn(Optional.empty());

        final User defaultUser = new User().withUserId(DEFAULT_USER_ID).withEmail(EMAIL);
        when(userPersistence.getUserByEmail(EMAIL)).thenReturn(Optional.of(defaultUser));

        final AuthenticatedUser newUser =
            new AuthenticatedUser().withUserId(UUID.randomUUID()).withEmail(EMAIL).withAuthUserId(NEW_AUTH_USER_ID)
                .withDefaultWorkspaceId(UUID.randomUUID());
        when(uuidSupplier.get()).thenReturn(newUser.getUserId());
        when(userPersistence.getUser(newUser.getUserId())).thenReturn(Optional.of(AuthenticatedUserConverter.toUser(newUser)));

        final UserGetOrCreateByAuthIdResponse res = userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID));
        assertTrue(res.getNewUserCreated());
        assertEquals(res.getUserRead().getUserId(), newUser.getUserId());
        assertEquals(res.getUserRead().getEmail(), EMAIL);
        assertEquals(res.getAuthUserId(), NEW_AUTH_USER_ID);

        verify(userPersistence).writeUser(defaultUser.withEmail(""));
        verify(userPersistence)
            .writeAuthenticatedUser(
                argThat(user -> user.getEmail().equals(jwtUser.getEmail()) && user.getAuthUserId().equals(jwtUser.getAuthUserId())));
      }

      @Test
      void testRelinkOrphanedUser() throws IOException, JsonValidationException, ConfigNotFoundException, PermissionRedundantException {
        // Auth user in JWT is not linked to any user in the database
        when(jwtUserAuthenticationResolver.resolveUser(NEW_AUTH_USER_ID)).thenReturn(jwtUser);
        when(userPersistence.getUserByAuthId(NEW_AUTH_USER_ID)).thenReturn(Optional.empty());

        // A user with the same email exists in the database
        when(userPersistence.getUserByEmail(EMAIL)).thenReturn(Optional.of(existingUser));
        when(userPersistence.getUser(EXISTING_USER_ID)).thenReturn(Optional.of(existingUser));

        // None of the auth users configured for the existing user actually exist in the external user
        // service
        when(userPersistence.listAuthUsersForUser(EXISTING_USER_ID))
            .thenReturn(List.of(new AuthUser().withAuthUserId(EXISTING_AUTH_USER_ID).withAuthProvider(AuthProvider.KEYCLOAK)));
        when(externalUserService.getRealmByAuthUserId(EXISTING_AUTH_USER_ID)).thenReturn(null);

        final UserGetOrCreateByAuthIdResponse res = userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID));
        assertFalse(res.getNewUserCreated());
        assertEquals(res.getUserRead().getUserId(), EXISTING_USER_ID);

        // verify auth user is replaced
        verify(userPersistence).replaceAuthUserForUserId(EXISTING_USER_ID, NEW_AUTH_USER_ID, AuthProvider.KEYCLOAK);
      }

      private static Stream<Arguments> ssoSignInArgsProvider() {
        return Stream.of(
            // Existing user is already an SSO user (will error):
            Arguments.of(true, false),

            // Existing user is regular user (will migrate):
            Arguments.of(false, true),
            Arguments.of(false, false));
      }

      @ParameterizedTest
      @MethodSource("ssoSignInArgsProvider")
      void testSSOSignInEmailExistsMigratesAuthUser(final boolean isExistingUserSSO, final boolean doesExistingUserHaveOrgPermission)
          throws IOException, JsonValidationException, ConfigNotFoundException, PermissionRedundantException {
        when(organizationPersistence.getOrganizationBySsoConfigRealm(SSO_REALM)).thenReturn(Optional.of(ORGANIZATION));

        when(jwtUserAuthenticationResolver.resolveUser(NEW_AUTH_USER_ID)).thenReturn(jwtUser);
        when(userPersistence.getUserByAuthId(NEW_AUTH_USER_ID)).thenReturn(Optional.empty());
        when(userPersistence.getUserByEmail(EMAIL)).thenReturn(Optional.of(existingUser));
        when(userPersistence.getUser(EXISTING_USER_ID)).thenReturn(Optional.of(existingUser));
        when(userPersistence.listAuthUsersForUser(EXISTING_USER_ID))
            .thenReturn(List.of(new AuthUser().withAuthUserId(EXISTING_AUTH_USER_ID).withAuthProvider(AuthProvider.KEYCLOAK)));

        if (isExistingUserSSO) {
          when(externalUserService.getRealmByAuthUserId(EXISTING_AUTH_USER_ID)).thenReturn(SSO_REALM);
          when(organizationPersistence.getSsoConfigByRealmName(SSO_REALM)).thenReturn(Optional.of(new SsoConfig()));

          assertThrows(UserAlreadyExistsProblem.class,
              () -> userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID)));
          return;
        }

        when(externalUserService.getRealmByAuthUserId(EXISTING_AUTH_USER_ID)).thenReturn(REALM);
        when(organizationPersistence.getSsoConfigByRealmName(REALM)).thenReturn(Optional.empty());

        when(userPersistence.listAuthUsersForUser(EXISTING_USER_ID))
            .thenReturn(List.of(new AuthUser().withAuthUserId(EXISTING_AUTH_USER_ID).withAuthProvider(AuthProvider.KEYCLOAK)));

        final AuthenticatedUser existingAuthedUser =
            AuthenticatedUserConverter.toAuthenticatedUser(existingUser, EXISTING_AUTH_USER_ID, AuthProvider.KEYCLOAK);

        when(applicationService.listApplicationsByUser(existingAuthedUser)).thenReturn(List.of(new Application().withId("app_id")));
        when(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(SSO_REALM);
        when(workspacesHandler
            .listWorkspacesInOrganization(new ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION.getOrganizationId())))
                .thenReturn(new WorkspaceReadList().workspaces(List.of(new WorkspaceRead().workspaceId(UUID.randomUUID()))));

        if (doesExistingUserHaveOrgPermission) {
          when(permissionHandler.listPermissionsForOrganization(ORGANIZATION.getOrganizationId()))
              .thenReturn(List.of(new UserPermission().withUser(existingUser)));
        } else {
          when(permissionHandler.listPermissionsForOrganization(ORGANIZATION.getOrganizationId()))
              .thenReturn(List.of(new UserPermission().withUser(new User().withUserId(UUID.randomUUID()))));
        }

        final UserGetOrCreateByAuthIdResponse res = userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID));
        assertFalse(res.getNewUserCreated());

        // verify apps are revoked
        verify(applicationService).deleteApplication(existingAuthedUser, "app_id");

        // verify auth user is replaced
        verify(userPersistence).replaceAuthUserForUserId(EXISTING_USER_ID, NEW_AUTH_USER_ID, AuthProvider.KEYCLOAK);

        // verify old auth user is deleted from other realms
        verify(externalUserService).deleteUserByEmailOnOtherRealms(EMAIL, SSO_REALM);

        // verify org permission is created (if it doesn't already exist)
        if (!doesExistingUserHaveOrgPermission) {
          verify(permissionHandler).createPermission(new Permission()
              .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
              .withOrganizationId(ORGANIZATION.getOrganizationId())
              .withUserId(EXISTING_USER_ID));
        }

        // verify user read
        final UserRead userRead = res.getUserRead();
        assertEquals(userRead.getUserId(), EXISTING_USER_ID);
        assertEquals(userRead.getEmail(), EMAIL);
      }

    }

    @Nested
    class NewUserTest {

      private static final String NEW_AUTH_USER_ID = "new_auth_user_id";
      private static final UUID NEW_USER_ID = UUID.randomUUID();
      private static final String NEW_EMAIL = "new@gmail.com";
      private static final UUID EXISTING_USER_ID = UUID.randomUUID();
      private static final String EXISTING_EMAIL = "existing@gmail.com";
      private static final UUID WORKSPACE_ID = UUID.randomUUID();

      private AuthenticatedUser newAuthedUser;
      private User newUser;
      private User existingUser;
      private WorkspaceRead defaultWorkspace;

      // this class provides the arguments for the parameterized test below, by returning all
      // permutations of auth provider, sso realm, initial user email, and deployment mode
      static class NewUserArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
          final List<AuthProvider> authProviders = Arrays.asList(AuthProvider.values());
          final List<String> authRealms = Arrays.asList("airbyte-realm", null);
          final List<String> initialUserEmails = Arrays.asList(null, "", "other@gmail.com", NEW_EMAIL);
          final List<UUID> domainRestrictedToOrgIds = Arrays.asList(null, UUID.randomUUID(), ORGANIZATION.getOrganizationId());
          final List<Boolean> initialUserConfigPresent = Arrays.asList(true, false);
          final List<Boolean> isFirstOrgUser = Arrays.asList(true, false);
          final List<Boolean> isDefaultWorkspaceForOrgPresent = Arrays.asList(true, false);

          // return all permutations of the above input lists so that we can test all combinations.
          return authProviders.stream()
              .flatMap(
                  authProvider -> authRealms.stream()
                      .flatMap(authRealm -> initialUserEmails.stream()
                          .flatMap(email -> initialUserConfigPresent.stream()
                              .flatMap(initialUserPresent -> isFirstOrgUser.stream()
                                  .flatMap(firstOrgUser -> isDefaultWorkspaceForOrgPresent.stream()
                                      .flatMap(orgWorkspacePresent -> domainRestrictedToOrgIds.stream()
                                          .flatMap(domainRestrictedToOrgId -> Stream.of(Arguments.of(
                                              authProvider, authRealm, email, initialUserPresent, firstOrgUser, orgWorkspacePresent,
                                              domainRestrictedToOrgId)))))))));
        }

      }

      @BeforeEach
      void setUp() throws IOException, JsonValidationException, ConfigNotFoundException {
        newAuthedUser = new AuthenticatedUser().withUserId(NEW_USER_ID).withEmail(NEW_EMAIL).withAuthUserId(NEW_AUTH_USER_ID);
        newUser = AuthenticatedUserConverter.toUser(newAuthedUser);
        existingUser = new User().withUserId(EXISTING_USER_ID).withEmail(EXISTING_EMAIL);
        defaultWorkspace = new WorkspaceRead().workspaceId(WORKSPACE_ID);
        when(userPersistence.getUserByAuthId(anyString())).thenReturn(Optional.empty());
        when(jwtUserAuthenticationResolver.resolveUser(NEW_AUTH_USER_ID)).thenReturn(newAuthedUser);
        when(uuidSupplier.get()).thenReturn(NEW_USER_ID);
        when(userPersistence.getUser(NEW_USER_ID)).thenReturn(Optional.of(newUser));
        when(resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any())).thenReturn(defaultWorkspace);
      }

      @ParameterizedTest
      @ArgumentsSource(NewUserArgumentsProvider.class)
      void testNewUserCreation(final AuthProvider authProvider,
                               final String authRealm,
                               final String initialUserEmail,
                               final boolean initialUserPresent,
                               final boolean isFirstOrgUser,
                               final boolean isDefaultWorkspaceForOrgPresent,
                               final UUID domainRestrictedToOrgId)
          throws Exception {

        newAuthedUser.setAuthProvider(authProvider);

        if (domainRestrictedToOrgId != null) {
          final String emailDomain = newUser.getEmail().split("@")[1];
          when(organizationEmailDomainService.findByEmailDomain(emailDomain))
              .thenReturn(List.of(new OrganizationEmailDomain()
                  .withOrganizationId(domainRestrictedToOrgId).withEmailDomain(emailDomain)));
        }

        when(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(authRealm);
        if (authRealm != null) {
          when(organizationPersistence.getOrganizationBySsoConfigRealm(authRealm)).thenReturn(Optional.of(ORGANIZATION));
        }

        if (initialUserPresent) {
          if (initialUserEmail != null) {
            when(initialUserConfig.getEmail()).thenReturn(initialUserEmail);
          }
        } else {
          // replace default user handler with one that doesn't use initial user config (ie to test what
          // happens in Cloud)
          userHandler = new UserHandler(userPersistence, externalUserService, organizationPersistence,
              organizationEmailDomainService, Optional.of(applicationService), permissionHandler, workspacesHandler,
              uuidSupplier, jwtUserAuthenticationResolver, Optional.empty(), resourceBootstrapHandler, featureFlagClient);
        }

        if (isFirstOrgUser) {
          when(permissionHandler.listPermissionsForOrganization(ORGANIZATION.getOrganizationId())).thenReturn(List.of());
        } else {
          // add a pre-existing admin user for the org if this isn't the first user
          final var existingUserPermission = new UserPermission()
              .withUser(existingUser)
              .withPermission(new Permission().withPermissionType(PermissionType.ORGANIZATION_ADMIN));

          when(permissionHandler.listPermissionsForOrganization(ORGANIZATION.getOrganizationId()))
              .thenReturn(List.of(existingUserPermission));
        }

        if (isDefaultWorkspaceForOrgPresent) {
          when(workspacesHandler.listWorkspacesInOrganization(
              new ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION.getOrganizationId()))).thenReturn(
                  new WorkspaceReadList().workspaces(List.of(defaultWorkspace)));
          if (newUser.getDefaultWorkspaceId() == null) {
            newUser.setDefaultWorkspaceId(defaultWorkspace.getWorkspaceId());
          }
        } else {
          when(workspacesHandler.listWorkspacesInOrganization(any())).thenReturn(new WorkspaceReadList().workspaces(List.of()));
        }

        final io.airbyte.api.model.generated.AuthProvider apiAuthProvider =
            Enums.convertTo(authProvider, io.airbyte.api.model.generated.AuthProvider.class);

        if (domainRestrictedToOrgId != null && (authRealm == null || domainRestrictedToOrgId != ORGANIZATION.getOrganizationId())) {
          assertThrows(SSORequiredProblem.class, () -> userHandler.getOrCreateUserByAuthId(new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID)));
          verify(userPersistence, never()).writeAuthenticatedUser(any());
          if (authRealm != null) {
            verify(externalUserService).deleteUserByExternalId(newAuthedUser.getAuthUserId(), authRealm);
          }
          return;
        }

        final UserGetOrCreateByAuthIdResponse response = userHandler.getOrCreateUserByAuthId(
            new UserAuthIdRequestBody().authUserId(NEW_AUTH_USER_ID));

        final InOrder userPersistenceInOrder = inOrder(userPersistence);

        assertTrue(response.getNewUserCreated());
        verifyCreatedUser(authProvider, userPersistenceInOrder);
        verifyUserRes(response, apiAuthProvider);
        verifyInstanceAdminPermissionCreation(initialUserEmail, initialUserPresent);
        verifyOrganizationPermissionCreation(authRealm, isFirstOrgUser);
        verifyDefaultWorkspaceCreation(isDefaultWorkspaceForOrgPresent, userPersistenceInOrder);
      }

      private void verifyCreatedUser(final AuthProvider expectedAuthProvider, final InOrder inOrder) throws IOException {
        inOrder.verify(userPersistence).writeAuthenticatedUser(argThat(user -> user.getUserId().equals(NEW_USER_ID)
            && NEW_EMAIL.equals(user.getEmail())
            && NEW_AUTH_USER_ID.equals(user.getAuthUserId())
            && user.getAuthProvider().equals(expectedAuthProvider)));
      }

      private void verifyDefaultWorkspaceCreation(final Boolean isDefaultWorkspaceForOrgPresent, final InOrder inOrder)
          throws IOException {
        // No need to deal with other vars because SSO users and first org users etc. are all directed
        // through the same codepath now.
        if (!isDefaultWorkspaceForOrgPresent) {
          // create a default workspace for the org if one doesn't yet exist
          verify(resourceBootstrapHandler).bootStrapWorkspaceForCurrentUser(any());
          // if a workspace was created, verify that the user's defaultWorkspaceId was updated
          // and that a workspaceAdmin permission was created for them.
          inOrder.verify(userPersistence).writeUser(argThat(user -> user.getDefaultWorkspaceId().equals(WORKSPACE_ID)));
        } else {
          // never create an additional workspace for the org if one already exists.
          verify(resourceBootstrapHandler, never()).bootStrapWorkspaceForCurrentUser(any());
        }
      }

      private void verifyUserRes(final UserGetOrCreateByAuthIdResponse userRes,
                                 final io.airbyte.api.model.generated.AuthProvider expectedAuthProvider) {
        final UserRead userRead = userRes.getUserRead();
        assertEquals(userRead.getUserId(), NEW_USER_ID);
        assertEquals(userRead.getEmail(), NEW_EMAIL);
        assertEquals(userRes.getAuthUserId(), NEW_AUTH_USER_ID);
        assertEquals(userRes.getAuthProvider(), expectedAuthProvider);
      }

      private void verifyInstanceAdminPermissionCreation(final String initialUserEmail, final boolean initialUserPresent)
          throws Exception {
        // instance_admin permissions should only ever be created when the initial user config is present
        // (which should never be true in Cloud).
        // also, if the initial user email is null or doesn't match the new user's email, no instance_admin
        // permission should be created
        if (!initialUserPresent || initialUserEmail == null || !initialUserEmail.equalsIgnoreCase(NEW_EMAIL)) {
          verify(permissionHandler, never())
              .createPermission(argThat(permission -> permission.getPermissionType().equals(PermissionType.INSTANCE_ADMIN)));
          verify(permissionHandler, never()).grantInstanceAdmin(any());
        } else {
          // otherwise, instance_admin permission should be created
          verify(permissionHandler).grantInstanceAdmin(any());
        }
      }

      private void verifyOrganizationPermissionCreation(final String ssoRealm, final boolean isFirstOrgUser)
          throws IOException, JsonValidationException, PermissionRedundantException {
        // if the SSO Realm is null, no organization permission should be created
        if (ssoRealm == null) {
          verify(permissionHandler, never()).createPermission(
              argThat(permission -> permission.getPermissionType().equals(Permission.PermissionType.ORGANIZATION_ADMIN)));
        } else {
          final Permission.PermissionType expectedPermissionType = isFirstOrgUser
              ? Permission.PermissionType.ORGANIZATION_ADMIN
              : Permission.PermissionType.ORGANIZATION_MEMBER;
          // otherwise, organization permission should be created for the associated user and org.
          verify(permissionHandler).createPermission(new Permission()
              .withPermissionType(expectedPermissionType)
              .withOrganizationId(ORGANIZATION.getOrganizationId())
              .withUserId(NEW_USER_ID));
        }
      }

    }

  }

}
