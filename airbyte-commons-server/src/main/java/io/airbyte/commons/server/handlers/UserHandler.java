/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.persistence.UserPersistence.DEFAULT_USER_ID;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AuthProvider;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserRead;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserEmailRequestBody;
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserStatus;
import io.airbyte.api.model.generated.UserUpdate;
import io.airbyte.api.model.generated.UserWithPermissionInfoRead;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUserRead;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
import io.airbyte.commons.auth.config.InitialUserConfiguration;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.support.UserAuthenticationResolver;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.User;
import io.airbyte.config.User.Status;
import io.airbyte.config.UserPermission;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserHandler, provides basic CRUD operation access for users. Some are migrated from Cloud
 * UserHandler.
 */
@Singleton
public class UserHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserHandler.class);

  private final Supplier<UUID> uuidGenerator;
  private final UserPersistence userPersistence;
  private final PermissionPersistence permissionPersistence;
  private final PermissionHandler permissionHandler;
  private final WorkspacesHandler workspacesHandler;
  private final OrganizationPersistence organizationPersistence;

  private final Optional<UserAuthenticationResolver> userAuthenticationResolver;
  private final Optional<InitialUserConfiguration> initialUserConfiguration;

  @VisibleForTesting
  public UserHandler(
                     final UserPersistence userPersistence,
                     final PermissionPersistence permissionPersistence,
                     final OrganizationPersistence organizationPersistence,
                     final PermissionHandler permissionHandler,
                     final WorkspacesHandler workspacesHandler,
                     @Named("uuidGenerator") final Supplier<UUID> uuidGenerator,
                     final Optional<UserAuthenticationResolver> userAuthenticationResolver,
                     final Optional<InitialUserConfiguration> initialUserConfiguration) {
    this.uuidGenerator = uuidGenerator;
    this.userPersistence = userPersistence;
    this.organizationPersistence = organizationPersistence;
    this.permissionPersistence = permissionPersistence;
    this.workspacesHandler = workspacesHandler;
    this.permissionHandler = permissionHandler;
    this.userAuthenticationResolver = userAuthenticationResolver;
    this.initialUserConfiguration = initialUserConfiguration;
  }

  /**
   * Creates a new user.
   *
   * @param userCreate The user to be created.
   * @return The newly created user.
   * @throws ConfigNotFoundException if unable to create the new user.
   * @throws IOException if unable to create the new user.
   * @throws JsonValidationException if unable to create the new user.
   */
  public UserRead createUser(final UserCreate userCreate) throws IOException, ConfigNotFoundException, JsonValidationException {

    final UserAuthIdRequestBody userAuthIdRequestBody = new UserAuthIdRequestBody().authUserId(userCreate.getAuthUserId());
    assertAuthIdHasNotBeenUsed(userAuthIdRequestBody);

    final UUID userId = userCreate.getUserId() != null ? userCreate.getUserId() : uuidGenerator.get();
    final User user = new User()
        .withName(userCreate.getName())
        .withUserId(userId)
        .withAuthUserId(userCreate.getAuthUserId())
        .withAuthProvider(Enums.convertTo(userCreate.getAuthProvider(), User.AuthProvider.class))
        .withStatus(Enums.convertTo(userCreate.getStatus(), User.Status.class))
        .withCompanyName(userCreate.getCompanyName())
        .withEmail(userCreate.getEmail())
        .withNews(userCreate.getNews());
    userPersistence.writeUser(user);
    return buildUserRead(userId);
  }

  private void assertAuthIdHasNotBeenUsed(final UserAuthIdRequestBody userAuthIdRequestBody) {
    UserRead userRead = null;
    try {
      userRead = getUserByAuthId(userAuthIdRequestBody);
    } catch (final ConfigNotFoundException e) {
      // This is "expected" if we want to create a new user.
      LOGGER.debug("Unable to find user with auth ID {}.", userAuthIdRequestBody.getAuthUserId());
    } catch (final IOException | JsonValidationException e) {
      LOGGER.error("Error checking if auth id in unique: {}.", e.toString());
      throw new InternalServerKnownException("Error performing auth id checks..", e);
    }
    if (userRead != null) {
      // The user has already existed. Avoid to create a dup user.
      throw new ValueConflictKnownException("Auth Id was already used to sign up");
    }
  }

  /**
   * Get a user by internal user ID.
   *
   * @param userIdRequestBody The internal user id to be queried.
   * @return The user.
   * @throws ConfigNotFoundException if unable to create the new user.
   * @throws IOException if unable to create the new user.
   * @throws JsonValidationException if unable to create the new user.
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
   * @throws JsonValidationException if unable to retrieve the user.
   */
  public UserRead getUserByAuthId(final UserAuthIdRequestBody userAuthIdRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final Optional<User> user = userPersistence.getUserByAuthId(userAuthIdRequestBody.getAuthUserId());
    if (user.isPresent()) {
      return buildUserRead(user.get());
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
   * @throws JsonValidationException if unable to retrieve the user.
   */
  public UserRead getUserByEmail(final UserEmailRequestBody userEmailRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final Optional<User> user = userPersistence.getUserByEmail(userEmailRequestBody.getEmail());
    if (user.isPresent()) {
      return buildUserRead(user.get());
    } else {
      throw new ConfigNotFoundException(ConfigSchema.USER, String.format("User not found by email request: %s", userEmailRequestBody));
    }
  }

  private UserRead buildUserRead(final UUID userId) throws ConfigNotFoundException, IOException, JsonValidationException {
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
        .authUserId(user.getAuthUserId())
        .authProvider(Enums.convertTo(user.getAuthProvider(), AuthProvider.class))
        .status(Enums.convertTo(user.getStatus(), UserStatus.class))
        .companyName(user.getCompanyName())
        .email(user.getEmail())
        .metadata(user.getUiMetadata())
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

    final User user = buildUser(userRead);

    // We do not allow update on these fields: userId, authUserId, authProvider.
    boolean hasUpdate = false;
    if (userUpdate.getName() != null) {
      user.setName(userUpdate.getName());
      hasUpdate = true;
    }

    if (userUpdate.getEmail() != null) {
      user.setEmail(userUpdate.getEmail());
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

    // TODO: allow to update UI metadata

    if (hasUpdate) {
      userPersistence.writeUser(user);
      return buildUserRead(userUpdate.getUserId());
    }
    throw new IllegalArgumentException(
        "Patch update user is not successful because there is nothing to update, or requested updating fields are not supported.");
  }

  private User buildUser(final UserRead userRead) {
    return new User()
        .withName(userRead.getName())
        .withUserId(userRead.getUserId())
        .withAuthUserId(userRead.getAuthUserId())
        .withAuthProvider(Enums.convertTo(userRead.getAuthProvider(), User.AuthProvider.class))
        .withDefaultWorkspaceId(userRead.getDefaultWorkspaceId())
        .withStatus(Enums.convertTo(userRead.getStatus(), Status.class))
        .withCompanyName(userRead.getCompanyName())
        .withEmail(userRead.getEmail())
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
        .authUserId(userRead.getAuthUserId())
        .authProvider(userRead.getAuthProvider())
        .status(UserStatus.DISABLED)
        .companyName(userRead.getCompanyName())
        .email(userRead.getEmail())
        .news(userRead.getNews());
    updateUser(userUpdate);
  }

  public OrganizationUserReadList listUsersInOrganization(final OrganizationIdRequestBody organizationIdRequestBody) throws IOException {
    final UUID organizationId = organizationIdRequestBody.getOrganizationId();
    final List<UserPermission> userPermissions = permissionPersistence.listUsersInOrganization(organizationId);
    return buildOrganizationUserReadList(userPermissions, organizationId);
  }

  public WorkspaceUserReadList listUsersInWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody) throws IOException {
    final UUID workspaceId = workspaceIdRequestBody.getWorkspaceId();
    final List<UserPermission> userPermissions = permissionPersistence.listUsersInWorkspace(workspaceId);
    return buildWorkspaceUserReadList(userPermissions, workspaceId);
  }

  public UserWithPermissionInfoReadList listInstanceAdminUsers() throws IOException {
    final List<UserPermission> userPermissions = permissionPersistence.listInstanceAdminUsers();
    return new UserWithPermissionInfoReadList().users(userPermissions.stream()
        .map(userPermission -> new UserWithPermissionInfoRead()
            .userId(userPermission.getUser().getUserId())
            .email(userPermission.getUser().getEmail())
            .name(userPermission.getUser().getName())
            .permissionId(userPermission.getPermission().getPermissionId()))
        .collect(Collectors.toList()));
  }

  public UserGetOrCreateByAuthIdResponse getOrCreateUserByAuthId(final UserAuthIdRequestBody userAuthIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {

    final Optional<User> existingUser = userPersistence.getUserByAuthId(userAuthIdRequestBody.getAuthUserId());

    if (existingUser.isPresent()) {
      return new UserGetOrCreateByAuthIdResponse()
          .userRead(buildUserRead(existingUser.get()))
          .newUserCreated(false);
    }

    final User incomingJwtUser = resolveIncomingJwtUser(userAuthIdRequestBody);
    final UserRead createdUser = createUserFromIncomingUser(incomingJwtUser, userAuthIdRequestBody);

    handleUserPermissionsAndWorkspace(createdUser);

    // refresh the user from the database in case anything changed during permission/workspace
    // modification
    final User updatedUser = userPersistence.getUser(createdUser.getUserId())
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.USER, createdUser.getUserId()));

    return new UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .newUserCreated(true);
  }

  private User resolveIncomingJwtUser(final UserAuthIdRequestBody userAuthIdRequestBody) throws ConfigNotFoundException {
    final String authUserId = userAuthIdRequestBody.getAuthUserId();
    if (userAuthenticationResolver.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.USER, authUserId);
    }
    final User incomingJwtUser = userAuthenticationResolver.get().resolveUser(authUserId);
    if (!incomingJwtUser.getAuthUserId().equals(userAuthIdRequestBody.getAuthUserId())) {
      throw new IllegalArgumentException("JWT token doesn't match the auth id from the request body.");
    }
    return incomingJwtUser;
  }

  private UserRead createUserFromIncomingUser(final User incomingUser, final UserAuthIdRequestBody userAuthIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final UserCreate userCreate = new UserCreate()
        .name(incomingUser.getName())
        .authUserId(userAuthIdRequestBody.getAuthUserId())
        .authProvider(Enums.convertTo(incomingUser.getAuthProvider(), AuthProvider.class))
        .email(incomingUser.getEmail());

    LOGGER.debug("Creating User: " + userCreate);

    return createUser(userCreate);
  }

  private void handleUserPermissionsAndWorkspace(final UserRead createdUser) throws IOException, JsonValidationException, ConfigNotFoundException {
    createInstanceAdminPermissionIfInitialUser(createdUser);
    final Optional<Organization> ssoOrg = getSsoOrganizationIfExists(createdUser.getUserId());

    if (ssoOrg.isPresent()) {
      handleSsoUser(createdUser, ssoOrg.get());
    } else {
      handleNonSsoUser(createdUser);
    }
  }

  private void handleSsoUser(final UserRead user, final Organization organization)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    // look for any existing user permissions for this organization. exclude the default user that comes
    // with the Airbyte installation, since we want the first real SSO user to be the org admin.
    final List<UserPermission> orgPermissionsExcludingDefaultUser =
        permissionPersistence.listPermissionsForOrganization(organization.getOrganizationId())
            .stream()
            .filter(userPermission -> !userPermission.getUser().getUserId().equals(DEFAULT_USER_ID))
            .toList();

    // If this is the first real user in the org, create a default workspace for them and make them an
    // org admin.
    if (orgPermissionsExcludingDefaultUser.isEmpty()) {
      createPermissionForUserAndOrg(user.getUserId(), organization.getOrganizationId(), PermissionType.ORGANIZATION_ADMIN);
    } else {
      createPermissionForUserAndOrg(user.getUserId(), organization.getOrganizationId(), PermissionType.ORGANIZATION_MEMBER);
    }

    // If this organization doesn't have a workspace yet, create one, and set it as the default
    // workspace for this user.
    final WorkspaceReadList orgWorkspaces = workspacesHandler.listWorkspacesInOrganization(
        new ListWorkspacesInOrganizationRequestBody().organizationId(organization.getOrganizationId()));

    if (orgWorkspaces.getWorkspaces().isEmpty()) {
      final WorkspaceRead defaultWorkspace = createDefaultWorkspaceForUser(user, Optional.of(organization));
      createPermissionForUserAndWorkspace(user.getUserId(), defaultWorkspace.getWorkspaceId(), PermissionType.WORKSPACE_ADMIN);
    }
  }

  private void handleNonSsoUser(final UserRead user) throws JsonValidationException, ConfigNotFoundException, IOException {
    final WorkspaceRead defaultWorkspace = createDefaultWorkspaceForUser(user, Optional.empty());
    createPermissionForUserAndWorkspace(user.getUserId(), defaultWorkspace.getWorkspaceId(), PermissionType.WORKSPACE_ADMIN);
  }

  private WorkspaceRead createDefaultWorkspaceForUser(final UserRead createdUser, final Optional<Organization> organization)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    final WorkspaceRead defaultWorkspace = workspacesHandler.createDefaultWorkspaceForUser(createdUser, organization);

    // set default workspace id in User table
    final UserUpdate userUpdateDefaultWorkspace = new UserUpdate()
        .userId(createdUser.getUserId())
        .defaultWorkspaceId(defaultWorkspace.getWorkspaceId());
    updateUser(userUpdateDefaultWorkspace);

    return defaultWorkspace;
  }

  private Optional<Organization> getSsoOrganizationIfExists(final UUID userId) throws IOException, ConfigNotFoundException {
    final String ssoRealm = userAuthenticationResolver.orElseThrow().resolveSsoRealm();
    if (ssoRealm != null) {
      final Optional<Organization> attachedOrganization = organizationPersistence.getOrganizationBySsoConfigRealm(ssoRealm);
      if (attachedOrganization.isPresent()) {
        return attachedOrganization;
      } else {
        LOGGER.error("New user with ID {} has an SSO realm {} but no Organization was found for it.", userId, ssoRealm);
        throw new ConfigNotFoundException(ConfigSchema.ORGANIZATION, ssoRealm);
      }
    }
    return Optional.empty();
  }

  private void createPermissionForUserAndOrg(final UUID userId, final UUID orgId, final PermissionType permissionType)
      throws IOException, JsonValidationException {
    permissionHandler.createPermission(new io.airbyte.api.model.generated.PermissionCreate()
        .organizationId(orgId)
        .userId(userId)
        .permissionType(permissionType));
  }

  private void createPermissionForUserAndWorkspace(final UUID userId, final UUID workspaceId, final PermissionType permissionType)
      throws IOException, JsonValidationException {
    permissionHandler.createPermission(new io.airbyte.api.model.generated.PermissionCreate()
        .workspaceId(workspaceId)
        .userId(userId)
        .permissionType(permissionType));
  }

  private void createInstanceAdminPermissionIfInitialUser(final UserRead createdUser) throws IOException, JsonValidationException {
    if (initialUserConfiguration.isEmpty()) {
      // do nothing if initial_user bean is not present.
      return;
    }

    final String initialEmailFromConfig = initialUserConfiguration.get().getEmail();

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

    permissionPersistence.writePermission(new Permission()
        .withPermissionId(uuidGenerator.get())
        .withUserId(createdUser.getUserId())
        .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
        .withOrganizationId(null)
        .withWorkspaceId(null));
  }

  private WorkspaceUserReadList buildWorkspaceUserReadList(final List<UserPermission> userPermissions, final UUID workspaceId) {
    // we exclude the default user from this list because we don't want to expose it in the UI
    return new WorkspaceUserReadList().users(userPermissions
        .stream()
        .filter(userPermission -> !userPermission.getUser().getUserId().equals(DEFAULT_USER_ID))
        .map(userPermission -> new WorkspaceUserRead()
            .userId(userPermission.getUser().getUserId())
            .email(userPermission.getUser().getEmail())
            .name(userPermission.getUser().getName())
            .isDefaultWorkspace(workspaceId.equals(userPermission.getUser().getDefaultWorkspaceId()))
            .workspaceId(workspaceId)
            .permissionId(userPermission.getPermission().getPermissionId())
            .permissionType(
                Enums.toEnum(userPermission.getPermission().getPermissionType().value(), io.airbyte.api.model.generated.PermissionType.class)
                    .get()))
        .collect(Collectors.toList()));
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

}
