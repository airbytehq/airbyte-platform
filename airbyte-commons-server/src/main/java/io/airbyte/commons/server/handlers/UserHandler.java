/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.persistence.UserPersistence.DEFAULT_USER_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AuthProvider;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserRead;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserEmailRequestBody;
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserStatus;
import io.airbyte.api.model.generated.UserUpdate;
import io.airbyte.api.model.generated.UserWithPermissionInfoRead;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceCreateWithId;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoRead;
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList;
import io.airbyte.api.problems.model.generated.ProblemEmailData;
import io.airbyte.api.problems.throwable.generated.SSORequiredProblem;
import io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem;
import io.airbyte.commons.auth.config.InitialUserConfig;
import io.airbyte.commons.auth.support.UserAuthenticationResolver;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.ConflictException;
import io.airbyte.commons.server.errors.OperationNotAllowedException;
import io.airbyte.commons.server.handlers.helpers.WorkspaceHelpersKt;
import io.airbyte.config.Application;
import io.airbyte.config.AuthUser;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Organization;
import io.airbyte.config.OrganizationEmailDomain;
import io.airbyte.config.Permission;
import io.airbyte.config.SsoConfig;
import io.airbyte.config.User;
import io.airbyte.config.User.Status;
import io.airbyte.config.UserPermission;
import io.airbyte.config.WorkspaceUserAccessInfo;
import io.airbyte.config.helpers.AuthenticatedUserConverter;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.SQLOperationNotAllowedException;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.services.ApplicationService;
import io.airbyte.data.services.ExternalUserService;
import io.airbyte.data.services.OrganizationEmailDomainService;
import io.airbyte.data.services.PermissionRedundantException;
import io.airbyte.featureflag.EmailAttribute;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RestrictLoginsForSSODomains;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserHandler, provides basic CRUD operation access for users. Some are migrated from Cloud
 * UserHandler.
 */
@Singleton
@SuppressWarnings("PMD.PreserveStackTrace")
public class UserHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserHandler.class);

  private final Supplier<UUID> uuidGenerator;
  private final UserPersistence userPersistence;
  private final ExternalUserService externalUserService;
  private final Optional<ApplicationService> applicationService;
  private final OrganizationEmailDomainService organizationEmailDomainService;
  private final PermissionHandler permissionHandler;
  private final WorkspacesHandler workspacesHandler;
  private final OrganizationPersistence organizationPersistence;
  private final FeatureFlagClient featureFlagClient;

  private final UserAuthenticationResolver userAuthenticationResolver;
  private final Optional<InitialUserConfig> initialUserConfig;
  private final ResourceBootstrapHandlerInterface resourceBootstrapHandler;

  @VisibleForTesting
  public UserHandler(
                     final UserPersistence userPersistence,
                     final ExternalUserService externalUserService,
                     final OrganizationPersistence organizationPersistence,
                     final OrganizationEmailDomainService organizationEmailDomainService,
                     final Optional<ApplicationService> applicationService,
                     final PermissionHandler permissionHandler,
                     final WorkspacesHandler workspacesHandler,
                     @Named("uuidGenerator") final Supplier<UUID> uuidGenerator,
                     final UserAuthenticationResolver userAuthenticationResolver,
                     final Optional<InitialUserConfig> initialUserConfig,
                     final ResourceBootstrapHandlerInterface resourceBootstrapHandler,
                     final FeatureFlagClient featureFlagClient) {
    this.uuidGenerator = uuidGenerator;
    this.userPersistence = userPersistence;
    this.externalUserService = externalUserService;
    this.organizationPersistence = organizationPersistence;
    this.organizationEmailDomainService = organizationEmailDomainService;
    this.applicationService = applicationService;
    this.workspacesHandler = workspacesHandler;
    this.permissionHandler = permissionHandler;
    this.userAuthenticationResolver = userAuthenticationResolver;
    this.initialUserConfig = initialUserConfig;
    this.resourceBootstrapHandler = resourceBootstrapHandler;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Get a user by internal user ID.
   *
   * @param userIdRequestBody The internal user id to be queried.
   * @return The user.
   * @throws ConfigNotFoundException if unable to get the user.
   * @throws IOException if unable to get the user.
   * @throws JsonValidationException if unable to get the user.
   */
  public UserRead getUser(final UserIdRequestBody userIdRequestBody) throws JsonValidationException, ConfigNotFoundException, IOException {
    return buildUserRead(userIdRequestBody.getUserId());
  }

  /**
   * Retrieves the user by auth ID.
   *
   * @param userAuthIdRequestBody The {@link UserAuthIdRequestBody} that contains the auth ID.
   * @return The user associated with the auth ID.
   * @throws IOException if unable to retrieve the user.
   */
  public UserRead getUserByAuthId(final UserAuthIdRequestBody userAuthIdRequestBody)
      throws IOException, ConfigNotFoundException {
    final Optional<AuthenticatedUser> user = userPersistence.getUserByAuthId(userAuthIdRequestBody.getAuthUserId());
    if (user.isPresent()) {
      return buildUserRead(AuthenticatedUserConverter.toUser(user.get()));
    } else {
      throw new ConfigNotFoundException(ConfigSchema.USER, String.format("User not found by auth request: %s", userAuthIdRequestBody));
    }
  }

  /**
   * Retrieves the user by email.
   *
   * @param userEmailRequestBody The {@link UserEmailRequestBody} that contains the email.
   * @return The user associated with the email.
   * @throws IOException if unable to retrieve the user.
   */
  public UserRead getUserByEmail(final UserEmailRequestBody userEmailRequestBody)
      throws IOException, ConfigNotFoundException {
    final Optional<User> user = userPersistence.getUserByEmail(userEmailRequestBody.getEmail());
    if (user.isPresent()) {
      return buildUserRead(user.get());
    } else {
      throw new ConfigNotFoundException(ConfigSchema.USER, String.format("User not found by email request: %s", userEmailRequestBody));
    }
  }

  private UserRead buildUserRead(final UUID userId) throws ConfigNotFoundException, IOException {
    final Optional<User> user = userPersistence.getUser(userId);
    if (user.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.USER, userId);
    }
    return buildUserRead(user.get());
  }

  private UserRead buildUserRead(final User user) {
    return new UserRead()
        .name(user.getName())
        .userId(user.getUserId())
        .status(Enums.convertTo(user.getStatus(), UserStatus.class))
        .companyName(user.getCompanyName())
        .email(user.getEmail())
        .metadata(user.getUiMetadata() != null ? user.getUiMetadata() : Map.of())
        .news(user.getNews())
        .defaultWorkspaceId(user.getDefaultWorkspaceId());
  }

  /**
   * Patch update a user object.
   *
   * @param userUpdate the user to update. Will only update requested fields as long as they are
   *        supported.
   * @return Updated user.
   * @throws ConfigNotFoundException If user not found.
   * @throws IOException If user update was not successful.
   * @throws JsonValidationException If input json was not expected.
   */
  public UserRead updateUser(final UserUpdate userUpdate) throws ConfigNotFoundException, IOException, JsonValidationException {

    final UserRead userRead = getUser(new UserIdRequestBody().userId(userUpdate.getUserId()));

    final User user = buildUserInfo(userRead);

    // We do not allow update on these fields: userId and email
    boolean hasUpdate = false;
    if (userUpdate.getName() != null) {
      user.setName(userUpdate.getName());
      hasUpdate = true;
    }

    if (userUpdate.getCompanyName() != null) {
      user.setCompanyName(userUpdate.getCompanyName());
      hasUpdate = true;
    }

    if (userUpdate.getStatus() != null) {
      user.setStatus(Enums.convertTo(userUpdate.getStatus(), Status.class));
      hasUpdate = true;
    }

    if (userUpdate.getNews() != null) {
      user.setNews(userUpdate.getNews());
      hasUpdate = true;
    }

    if (userUpdate.getDefaultWorkspaceId() != null) {
      user.setDefaultWorkspaceId(userUpdate.getDefaultWorkspaceId());
      hasUpdate = true;
    }

    if (userUpdate.getMetadata() != null) {
      user.setUiMetadata(Jsons.convertValue(userUpdate.getMetadata(), JsonNode.class));
      hasUpdate = true;
    }

    if (hasUpdate) {
      userPersistence.writeUser(user);
      return buildUserRead(userUpdate.getUserId());
    }
    throw new IllegalArgumentException(
        "Patch update user is not successful because there is nothing to update, or requested updating fields are not supported.");
  }

  private User buildUserInfo(final UserRead userRead) {
    return new User()
        .withName(userRead.getName())
        .withUserId(userRead.getUserId())
        .withDefaultWorkspaceId(userRead.getDefaultWorkspaceId())
        .withStatus(Enums.convertTo(userRead.getStatus(), Status.class))
        .withCompanyName(userRead.getCompanyName())
        .withEmail(userRead.getEmail())
        .withUiMetadata(Jsons.jsonNode(userRead.getMetadata() != null ? userRead.getMetadata() : Map.of()))
        .withNews(userRead.getNews());
  }

  /**
   * Deletes a User.
   *
   * @param userIdRequestBody The user to be deleted.
   * @throws IOException if unable to delete the user.
   * @throws ConfigNotFoundException if unable to delete the user.
   */
  public void deleteUser(final UserIdRequestBody userIdRequestBody) throws ConfigNotFoundException, IOException, JsonValidationException {
    final UserRead userRead = getUser(userIdRequestBody);
    deleteUser(userRead);
  }

  private void deleteUser(final UserRead userRead) throws ConfigNotFoundException, IOException, JsonValidationException {
    final UserUpdate userUpdate = new UserUpdate()
        .name(userRead.getName())
        .userId(userRead.getUserId())
        .status(UserStatus.DISABLED)
        .companyName(userRead.getCompanyName())
        .news(userRead.getNews());
    updateUser(userUpdate);
  }

  public OrganizationUserReadList listUsersInOrganization(final OrganizationIdRequestBody organizationIdRequestBody) throws IOException {
    final UUID organizationId = organizationIdRequestBody.getOrganizationId();
    final List<UserPermission> userPermissions = permissionHandler.listUsersInOrganization(organizationId);
    return buildOrganizationUserReadList(userPermissions, organizationId);
  }

  public WorkspaceUserAccessInfoReadList listAccessInfoByWorkspaceId(final WorkspaceIdRequestBody workspaceIdRequestBody) throws IOException {
    final UUID workspaceId = workspaceIdRequestBody.getWorkspaceId();
    final List<WorkspaceUserAccessInfo> userAccessInfo = userPersistence.listWorkspaceUserAccessInfo(workspaceId);
    return buildWorkspaceUserAccessInfoReadList(userAccessInfo);
  }

  public UserWithPermissionInfoReadList listInstanceAdminUsers() throws IOException {
    final List<UserPermission> userPermissions = permissionHandler.listInstanceAdminUsers();
    return new UserWithPermissionInfoReadList().users(userPermissions.stream()
        .map(userPermission -> new UserWithPermissionInfoRead()
            .userId(userPermission.getUser().getUserId())
            .email(userPermission.getUser().getEmail())
            .name(userPermission.getUser().getName())
            .permissionId(userPermission.getPermission().getPermissionId()))
        .collect(Collectors.toList()));
  }

  private boolean isAllowedDomain(final String email) throws IOException {
    if (!featureFlagClient.boolVariation(RestrictLoginsForSSODomains.INSTANCE,
        new io.airbyte.featureflag.User(UUID.randomUUID(), new EmailAttribute(email)))) {
      return true;
    }

    final String emailDomain = email.split("@")[1];
    final List<OrganizationEmailDomain> restrictedForOrganizations = organizationEmailDomainService.findByEmailDomain(emailDomain);

    if (restrictedForOrganizations.isEmpty()) {
      return true;
    }

    final Optional<Organization> currentSSOOrg = getSsoOrganizationIfExists();
    return currentSSOOrg.isPresent() && restrictedForOrganizations.stream()
        .anyMatch(orgEmailDomain -> orgEmailDomain.getOrganizationId().equals(currentSSOOrg.get().getOrganizationId()));
  }

  private List<String> getExistingUserRealms(final UUID userId) throws IOException {
    final List<AuthUser> keycloakAuthUsers = userPersistence.listAuthUsersForUser(userId).stream()
        .filter(authUser -> authUser.getAuthProvider() == io.airbyte.config.AuthProvider.KEYCLOAK).toList();

    // Note: it's important to reach out to keycloak here to validate that at least one auth user from
    // our db actually exists in keycloak.
    return keycloakAuthUsers.stream()
        .map(authUser -> externalUserService.getRealmByAuthUserId(authUser.getAuthUserId()))
        .filter(Objects::nonNull)
        .toList();
  }

  private boolean isAnyRealmSSO(final List<String> realms) throws IOException {
    for (final String realm : realms) {
      final Optional<SsoConfig> ssoConfig = organizationPersistence.getSsoConfigByRealmName(realm);
      if (ssoConfig.isPresent()) {
        return true;
      }
    }

    return false;
  }

  private void handleSSORestrictions(final AuthenticatedUser incomingJwtUser, final boolean authUserExists) throws IOException {
    final boolean allowDomain = isAllowedDomain(incomingJwtUser.getEmail());
    if (!allowDomain) {
      if (!authUserExists) {
        // Keep keycloak clean by deleting the user if it doesn't exist in our auth_user table is not
        // allowed to sign in
        final String authRealm = userAuthenticationResolver.resolveRealm();
        if (authRealm != null) {
          externalUserService.deleteUserByExternalId(incomingJwtUser.getAuthUserId(), authRealm);
        }
      }
      throw new SSORequiredProblem();
    }
  }

  private UserGetOrCreateByAuthIdResponse handleNewUserLogin(final AuthenticatedUser incomingJwtUser)
      throws JsonValidationException, ConfigNotFoundException, IOException, PermissionRedundantException {
    final UserRead createdUser = createUserFromIncomingUser(incomingJwtUser);
    handleUserPermissionsAndWorkspace(createdUser);

    // refresh the user from the database in case anything changed during permission/workspace
    // modification
    final User updatedUser = userPersistence.getUser(createdUser.getUserId())
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.USER, createdUser.getUserId()));

    return new UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(incomingJwtUser.getAuthUserId())
        .authProvider(Enums.convertTo(incomingJwtUser.getAuthProvider(), AuthProvider.class))
        .newUserCreated(true);
  }

  private UserGetOrCreateByAuthIdResponse handleRelinkAuthUser(final User existingUser, final AuthenticatedUser incomingJwtUser)
      throws IOException, ConfigNotFoundException {
    LOGGER.info("Relinking auth user {} to orphaned existing user {}...", incomingJwtUser.getAuthUserId(), existingUser.getUserId());
    userPersistence.replaceAuthUserForUserId(existingUser.getUserId(), incomingJwtUser.getAuthUserId(), incomingJwtUser.getAuthProvider());

    final User updatedUser = userPersistence.getUser(existingUser.getUserId())
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.USER, existingUser.getUserId()));

    return new UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(incomingJwtUser.getAuthUserId())
        .authProvider(Enums.convertTo(incomingJwtUser.getAuthProvider(), AuthProvider.class))
        .newUserCreated(false);
  }

  private UserGetOrCreateByAuthIdResponse handleFirstTimeSSOLogin(final User existingUser, final AuthenticatedUser incomingJwtUser)
      throws JsonValidationException, ConfigNotFoundException, IOException, PermissionRedundantException {
    LOGGER.info("Migrating existing user {} to SSO...", existingUser.getUserId());
    // (1) Revoke existing applications
    if (applicationService.isPresent()) {
      final ApplicationService appService = applicationService.get();
      LOGGER.info("Revoking existing applications for user {}...", existingUser.getUserId());
      final List<AuthUser> authUsers = userPersistence.listAuthUsersForUser(existingUser.getUserId());
      for (final AuthUser authUser : authUsers) {
        final AuthenticatedUser authedUser =
            AuthenticatedUserConverter.toAuthenticatedUser(existingUser, authUser.getAuthUserId(), authUser.getAuthProvider());
        final List<Application> existingApplications = appService.listApplicationsByUser(authedUser);
        for (final Application application : existingApplications) {
          appService.deleteApplication(authedUser, application.getId());
          LOGGER.info("Revoked application {} for user {} (auth user {})...", application.getId(), existingUser.getUserId(),
              authUser.getAuthUserId());
        }
      }
    }

    // (2) Delete the user from other auth realms
    LOGGER.info("Deleting user with email {} from other auth realms...", existingUser.getEmail());
    final String newRealm = userAuthenticationResolver.resolveRealm();
    if (newRealm == null) {
      throw new IllegalStateException("No new realm found for user " + existingUser.getUserId());
    }
    externalUserService.deleteUserByEmailOnOtherRealms(existingUser.getEmail(), newRealm);

    // (3) Replace the existing auth user with the new one
    LOGGER.info("Replacing existing auth users with new one ({})...", incomingJwtUser.getAuthUserId());
    userPersistence.replaceAuthUserForUserId(existingUser.getUserId(), incomingJwtUser.getAuthUserId(), incomingJwtUser.getAuthProvider());

    LOGGER.info("Done migrating user {} to SSO", existingUser.getUserId());

    // (4) Return the user
    final UserRead userRead = buildUserRead(existingUser);
    handleUserPermissionsAndWorkspace(userRead);

    // refresh the user from the database in case anything changed during permission/workspace
    // modification
    final User updatedUser = userPersistence.getUser(userRead.getUserId())
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.USER, userRead.getUserId()));

    return new UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(incomingJwtUser.getAuthUserId())
        .authProvider(Enums.convertTo(incomingJwtUser.getAuthProvider(), AuthProvider.class))
        .newUserCreated(false);
  }

  public UserGetOrCreateByAuthIdResponse getOrCreateUserByAuthId(final UserAuthIdRequestBody userAuthIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException, PermissionRedundantException {
    final AuthenticatedUser incomingJwtUser = resolveIncomingJwtUser(userAuthIdRequestBody);
    final Optional<AuthenticatedUser> existingAuthUser = userPersistence.getUserByAuthId(userAuthIdRequestBody.getAuthUserId());

    // (1) Restrict logins for SSO domains
    handleSSORestrictions(incomingJwtUser, existingAuthUser.isPresent());

    // (2) Authenticate existing auth_user
    if (existingAuthUser.isPresent()) {
      return new UserGetOrCreateByAuthIdResponse()
          .userRead(buildUserRead(AuthenticatedUserConverter.toUser(existingAuthUser.get())))
          .authUserId(userAuthIdRequestBody.getAuthUserId())
          .authProvider(Enums.convertTo(incomingJwtUser.getAuthProvider(), AuthProvider.class))
          .newUserCreated(false);
    }

    // (3) Handle non-existing auth_user

    Optional<User> existingUserWithEmail = userPersistence.getUserByEmail(incomingJwtUser.getEmail());
    if (existingUserWithEmail.isPresent() && existingUserWithEmail.get().getUserId() == DEFAULT_USER_ID) {
      // (Enterprise) If the email is already taken by the default user, we can safely clear it so the
      // real user can be created
      userPersistence.writeUser(existingUserWithEmail.get().withEmail(""));
      LOGGER.info("Cleared email for default user on first login for {}", incomingJwtUser.getEmail());

      existingUserWithEmail = Optional.empty();
    }

    // (3a) Email has not been used before
    if (existingUserWithEmail.isEmpty()) {
      return handleNewUserLogin(incomingJwtUser);
    }

    // (3b) A user with the same email already exists
    final User existingUser = existingUserWithEmail.get();
    final List<String> existingUserRealms = getExistingUserRealms(existingUser.getUserId());

    // (3b0) The existing user does not exist in any auth realm, relink it
    // This can happen if, for example, keycloak state is cleared on an enterprise installation
    if (existingUserRealms.isEmpty()) {
      return handleRelinkAuthUser(existingUser, incomingJwtUser);
    }

    final boolean isCurrentSignInSSO = getSsoOrganizationIfExists().isPresent();
    final boolean isExistingUserSSOAuthed = isAnyRealmSSO(existingUserRealms);

    // (3b1) This is the first SSO sign in for the user, migrate it for SSO
    if (isCurrentSignInSSO && !isExistingUserSSOAuthed) {
      return handleFirstTimeSSOLogin(existingUser, incomingJwtUser);
    }

    // (3b2) This isn't a first-time SSO sign in and/or the user already exists
    final var realm = userAuthenticationResolver.resolveRealm();
    if (realm != null) {
      externalUserService.deleteUserByExternalId(incomingJwtUser.getAuthUserId(), realm);
    }
    throw new UserAlreadyExistsProblem(new ProblemEmailData().email(existingUser.getEmail()));
  }

  private AuthenticatedUser resolveIncomingJwtUser(final UserAuthIdRequestBody userAuthIdRequestBody) {
    final String authUserId = userAuthIdRequestBody.getAuthUserId();
    return userAuthenticationResolver.resolveUser(authUserId);
  }

  private UserRead createUserFromIncomingUser(final AuthenticatedUser incomingUser)
      throws ConfigNotFoundException, IOException {

    final UUID userId = uuidGenerator.get();
    final AuthenticatedUser user = incomingUser.withUserId(userId);

    LOGGER.debug("Creating User: {}", user);

    try {
      userPersistence.writeAuthenticatedUser(user);
    } catch (final DataAccessException e) {
      if (e.getCause() instanceof SQLOperationNotAllowedException) {
        throw new OperationNotAllowedException(e.getCause().getMessage());
      } else {
        throw new IOException(e);
      }
    }
    return buildUserRead(userId);
  }

  private void handleUserPermissionsAndWorkspace(final UserRead createdUser)
      throws IOException, JsonValidationException, ConfigNotFoundException, PermissionRedundantException {
    createInstanceAdminPermissionIfInitialUser(createdUser);
    final Optional<Organization> ssoOrg = getSsoOrganizationIfExists();
    if (ssoOrg.isPresent()) {
      // SSO users will have some additional logic but will ultimately call createDefaultWorkspaceForUser
      handleSsoUser(createdUser, ssoOrg.get());
    } else {
      // non-SSO users will just create a default workspace
      createDefaultWorkspaceForUser(createdUser, Optional.empty());
    }
  }

  private void handleSsoUser(final UserRead user, final Organization organization)
      throws IOException, JsonValidationException, ConfigNotFoundException, PermissionRedundantException {
    // look for any existing user permissions for this organization. exclude the default user that comes
    // with the Airbyte installation, since we want the first real SSO user to be the org admin.
    final List<UserPermission> orgPermissionsExcludingDefaultUser =
        permissionHandler.listPermissionsForOrganization(organization.getOrganizationId())
            .stream()
            .filter(userPermission -> !userPermission.getUser().getUserId().equals(DEFAULT_USER_ID))
            .toList();

    // If this is the first real user in the org, create a default workspace for them and make them an
    // org admin.
    if (orgPermissionsExcludingDefaultUser.isEmpty()) {
      createPermissionForUserAndOrg(user.getUserId(), organization.getOrganizationId(), Permission.PermissionType.ORGANIZATION_ADMIN);
    } else {
      final UUID userId = user.getUserId();
      final boolean hasOrgPermission = orgPermissionsExcludingDefaultUser.stream()
          .anyMatch(userPermission -> userPermission.getUser().getUserId().equals(userId));
      // check to avoid creating duplicate permissions
      if (!hasOrgPermission) {
        createPermissionForUserAndOrg(userId, organization.getOrganizationId(), Permission.PermissionType.ORGANIZATION_MEMBER);
      }
    }

    // If this organization doesn't have a workspace yet, create one, and set it as the default
    // workspace for this user.
    final WorkspaceReadList orgWorkspaces = workspacesHandler.listWorkspacesInOrganization(
        new ListWorkspacesInOrganizationRequestBody().organizationId(organization.getOrganizationId()));

    if (orgWorkspaces.getWorkspaces().isEmpty()) {
      // Now calls bootstrap which includes all permissions and updates userRead.
      createDefaultWorkspaceForUser(user, Optional.of(organization));
    }
  }

  protected void createDefaultWorkspaceForUser(final UserRead user, final Optional<Organization> organization)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    // Only do this if the user doesn't already have a default workspace.
    if (user.getDefaultWorkspaceId() != null) {
      return;
    }

    // Logic stolen from workspaceHandler.createDefaultWorkspaceForUser
    final String companyName = user.getCompanyName();
    final String email = user.getEmail();
    final Boolean news = user.getNews();
    // otherwise, create a default workspace for this user
    final WorkspaceCreateWithId workspaceCreate = new WorkspaceCreateWithId()
        .name(WorkspaceHelpersKt.getDefaultWorkspaceName(organization, companyName, email))
        .organizationId(organization.map(Organization::getOrganizationId).orElse(null))
        .email(email)
        .news(news)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .displaySetupWizard(true)
        .id(uuidGenerator.get());

    final WorkspaceRead defaultWorkspace = resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(workspaceCreate);

    // set default workspace id in User table
    final UserUpdate userUpdateDefaultWorkspace = new UserUpdate()
        .userId(user.getUserId())
        .defaultWorkspaceId(defaultWorkspace.getWorkspaceId());
    updateUser(userUpdateDefaultWorkspace);

  }

  private Optional<Organization> getSsoOrganizationIfExists() throws IOException {
    final var authRealm = userAuthenticationResolver.resolveRealm();
    if (authRealm == null) {
      return Optional.empty();
    }

    return organizationPersistence.getOrganizationBySsoConfigRealm(authRealm);
  }

  private void createPermissionForUserAndOrg(final UUID userId, final UUID orgId, final Permission.PermissionType permissionType)
      throws IOException, JsonValidationException, PermissionRedundantException {
    permissionHandler.createPermission(new Permission()
        .withOrganizationId(orgId)
        .withUserId(userId)
        .withPermissionType(permissionType));
  }

  private void createInstanceAdminPermissionIfInitialUser(final UserRead createdUser) {
    if (initialUserConfig.isEmpty()) {
      // do nothing if initial_user bean is not present.
      return;
    }

    final String initialEmailFromConfig = initialUserConfig.get().getEmail();

    if (initialEmailFromConfig == null || initialEmailFromConfig.isEmpty()) {
      // do nothing if there is no initial_user email configured.
      return;
    }

    // compare emails with case insensitivity because different email cases should be treated as the
    // same user.
    if (!initialEmailFromConfig.equalsIgnoreCase(createdUser.getEmail())) {
      return;
    }

    LOGGER.info("creating instance_admin permission for user ID {} because their email matches this instance's configured initial_user",
        createdUser.getUserId());

    try {
      permissionHandler.grantInstanceAdmin(createdUser.getUserId());
    } catch (final PermissionRedundantException e) {
      throw new ConflictException(e.getMessage(), e);
    }
  }

  private OrganizationUserReadList buildOrganizationUserReadList(final List<UserPermission> userPermissions, final UUID organizationId) {
    // we exclude the default user from this list because we don't want to expose it in the UI
    return new OrganizationUserReadList().users(userPermissions
        .stream()
        .filter(userPermission -> !userPermission.getUser().getUserId().equals(DEFAULT_USER_ID))
        .map(userPermission -> new OrganizationUserRead()
            .userId(userPermission.getUser().getUserId())
            .email(userPermission.getUser().getEmail())
            .name(userPermission.getUser().getName())
            .organizationId(organizationId)
            .permissionId(userPermission.getPermission().getPermissionId())
            .permissionType(
                Enums.toEnum(userPermission.getPermission().getPermissionType().value(), io.airbyte.api.model.generated.PermissionType.class).get()))
        .collect(Collectors.toList()));
  }

  private WorkspaceUserAccessInfoReadList buildWorkspaceUserAccessInfoReadList(final List<WorkspaceUserAccessInfo> accessInfos) {
    // we exclude the default user from this list because we don't want to expose it in the UI
    return new WorkspaceUserAccessInfoReadList().usersWithAccess(accessInfos
        .stream()
        .filter(accessInfo -> !accessInfo.getUserId().equals(DEFAULT_USER_ID))
        .map(this::buildWorkspaceUserAccessInfoRead)
        .collect(Collectors.toList()));
  }

  private WorkspaceUserAccessInfoRead buildWorkspaceUserAccessInfoRead(final WorkspaceUserAccessInfo accessInfo) {
    final PermissionRead workspacePermissionRead = Optional.ofNullable(accessInfo.getWorkspacePermission())
        .map(wp -> new PermissionRead()
            .permissionId(wp.getPermissionId())
            .permissionType(Enums.convertTo(wp.getPermissionType(), PermissionType.class))
            .userId(wp.getUserId())
            .workspaceId(wp.getWorkspaceId()))
        .orElse(null);

    final PermissionRead organizationPermissionRead = Optional.ofNullable(accessInfo.getOrganizationPermission())
        .map(op -> new PermissionRead()
            .permissionId(op.getPermissionId())
            .permissionType(Enums.convertTo(op.getPermissionType(), PermissionType.class))
            .userId(op.getUserId())
            .organizationId(op.getOrganizationId()))
        .orElse(null);

    return new WorkspaceUserAccessInfoRead()
        .userId(accessInfo.getUserId())
        .userEmail(accessInfo.getUserEmail())
        .userName(accessInfo.getUserName())
        .workspaceId(accessInfo.getWorkspaceId())
        .workspacePermission(workspacePermissionRead)
        .organizationPermission(organizationPermissionRead);
  }

}
