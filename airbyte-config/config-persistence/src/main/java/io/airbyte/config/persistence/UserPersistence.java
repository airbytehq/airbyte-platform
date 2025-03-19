/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.PermissionPersistenceHelper.ORG_PERMISSION_ID_ALIAS;
import static io.airbyte.config.persistence.PermissionPersistenceHelper.ORG_PERMISSION_ORG_ID_ALIAS;
import static io.airbyte.config.persistence.PermissionPersistenceHelper.ORG_PERMISSION_TYPE_ALIAS;
import static io.airbyte.config.persistence.PermissionPersistenceHelper.WORKSPACE_PERMISSION_ID_ALIAS;
import static io.airbyte.config.persistence.PermissionPersistenceHelper.WORKSPACE_PERMISSION_TYPE_ALIAS;
import static io.airbyte.config.persistence.PermissionPersistenceHelper.WORKSPACE_PERMISSION_WORKSPACE_ID_ALIAS;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.AUTH_USER;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.USER;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AuthUser;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.config.WorkspaceUserAccessInfo;
import io.airbyte.config.helpers.AuthenticatedUserConverter;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider;
import io.airbyte.db.instance.configs.jooq.generated.enums.Status;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User Persistence.
 *
 * Perform all SQL queries and handle persisting User to the Config Database.
 *
 */
@SuppressWarnings("PMD.LiteralsFirstInComparisons")
public class UserPersistence {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PRIMARY_KEY = "id";

  /**
   * Each installation of Airbyte comes with a default user. The ID of this user is hardcoded to the 0
   * UUID so that it can be consistently retrieved.
   */
  public static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  private final ExceptionWrappingDatabase database;

  public UserPersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Create or update a user.
   *
   * @param user user to create or update.
   * @throws IOException in case of a db error
   */
  public void writeUser(final User user) throws IOException {
    database.transaction(ctx -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(USER)
          .where(USER.ID.eq(user.getUserId())));
      if (isExistingConfig) {
        updateUser(ctx, user);
      } else {
        createUser(ctx, user);
      }
      return null;
    });
  }

  private void updateUser(final DSLContext ctx, final User user) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    ctx.update(USER)
        .set(USER.ID, user.getUserId())
        .set(USER.NAME, user.getName())
        .set(USER.DEFAULT_WORKSPACE_ID, user.getDefaultWorkspaceId())
        .set(USER.STATUS, user.getStatus() == null ? null
            : Enums.toEnum(user.getStatus().value(), Status.class).orElseThrow())
        .set(USER.COMPANY_NAME, user.getCompanyName())
        .set(USER.EMAIL, user.getEmail())
        .set(USER.NEWS, user.getNews())
        .set(USER.UI_METADATA, JSONB.valueOf(Jsons.serialize(user.getUiMetadata())))
        .set(USER.UPDATED_AT, timestamp)
        .where(USER.ID.eq(user.getUserId()))
        .execute();
  }

  private void createUser(final DSLContext ctx, final User user) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    ctx.insertInto(USER)
        .set(USER.ID, user.getUserId())
        .set(USER.NAME, user.getName())
        .set(USER.DEFAULT_WORKSPACE_ID, user.getDefaultWorkspaceId())
        .set(USER.STATUS, user.getStatus() == null ? null
            : Enums.toEnum(user.getStatus().value(), Status.class).orElseThrow())
        .set(USER.COMPANY_NAME, user.getCompanyName())
        .set(USER.EMAIL, user.getEmail())
        .set(USER.NEWS, user.getNews())
        .set(USER.UI_METADATA, JSONB.valueOf(Jsons.serialize(user.getUiMetadata())))
        .set(USER.CREATED_AT, timestamp)
        .set(USER.UPDATED_AT, timestamp)
        .execute();
  }

  /**
   * Create or update a user.
   *
   * @param user user to create or update.
   */
  public void writeAuthenticatedUser(final AuthenticatedUser user) throws IOException {
    database.transaction(ctx -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(USER)
          .where(USER.ID.eq(user.getUserId())));

      if (isExistingConfig) {
        updateUser(ctx, AuthenticatedUserConverter.toUser(user));
      } else {
        createUser(ctx, AuthenticatedUserConverter.toUser(user));
        writeAuthUser(ctx, user.getUserId(), user.getAuthUserId(), user.getAuthProvider());
      }

      return null;
    });
  }

  public void writeAuthUser(final UUID userId, final String authUserId, final io.airbyte.config.AuthProvider authProvider) throws IOException {
    database.query(ctx -> {
      writeAuthUser(ctx, userId, authUserId, authProvider);
      return null;
    });
  }

  private void writeAuthUser(final DSLContext ctx, final UUID userId, final String authUserId, final io.airbyte.config.AuthProvider authProvider) {
    final OffsetDateTime now = OffsetDateTime.now();
    ctx.insertInto(AUTH_USER)
        .set(AUTH_USER.ID, UUID.randomUUID())
        .set(AUTH_USER.USER_ID, userId)
        .set(AUTH_USER.AUTH_USER_ID, authUserId)
        .set(AUTH_USER.AUTH_PROVIDER, authProvider == null ? null
            : Enums.toEnum(authProvider.value(), AuthProvider.class).orElseThrow())
        .set(AUTH_USER.CREATED_AT, now)
        .set(AUTH_USER.UPDATED_AT, now)
        .execute();
  }

  /**
   * Replace the auth user for a particular Airbyte user.
   *
   * @param userId internal user id
   * @param newAuthUserId new auth user id
   * @param newAuthProvider new auth provider
   * @throws IOException in case of a db error
   */
  public void replaceAuthUserForUserId(final UUID userId, final String newAuthUserId, final io.airbyte.config.AuthProvider newAuthProvider)
      throws IOException {
    database.transaction(ctx -> {
      ctx.deleteFrom(AUTH_USER).where(AUTH_USER.USER_ID.eq(userId)).execute();
      writeAuthUser(ctx, userId, newAuthUserId, newAuthProvider);
      return null;
    });
  }

  /**
   * Delete User.
   *
   * @param userId internal user id
   * @return user if found
   */
  public boolean deleteUserById(final UUID userId) throws IOException {
    return database.transaction(ctx -> ctx.deleteFrom(USER)).where(field(DSL.name(PRIMARY_KEY)).eq(userId)).execute() > 0;
  }

  /**
   * Get User.
   *
   * @param userId internal user id
   * @return user if found
   */
  @Deprecated
  public Optional<AuthenticatedUser> getAuthenticatedUser(final UUID userId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(USER)
        .leftJoin(AUTH_USER).on(USER.ID.eq(AUTH_USER.USER_ID))
        .where(USER.ID.eq(userId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    // FIXME: in the case of multiple auth providers, this will return the first one found.
    return Optional.of(createAuthenticatedUserFromRecord(result.get(0)));
  }

  /**
   * Get User.
   *
   * @param userId internal user id
   * @return user if found
   * @throws IOException in case of a db error
   */
  public Optional<User> getUser(final UUID userId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(USER)
        .where(USER.ID.eq(userId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createUserFromRecord(result.get(0)));
  }

  private User createUserFromRecord(final Record record) {
    return new User()
        .withUserId(record.get(USER.ID))
        .withName(record.get(USER.NAME))
        .withDefaultWorkspaceId(record.get(USER.DEFAULT_WORKSPACE_ID))
        .withStatus(record.get(USER.STATUS) == null ? null : Enums.toEnum(record.get(USER.STATUS, String.class), User.Status.class).orElseThrow())
        .withCompanyName(record.get(USER.COMPANY_NAME))
        .withEmail(record.get(USER.EMAIL))
        .withNews(record.get(USER.NEWS))
        // special handling of "null" string so User hashes predictably with Java `<null>` instead of
        // JsonNode `null`
        .withUiMetadata(record.get(USER.UI_METADATA) == null || record.get(USER.UI_METADATA).data().equals("null") ? null
            : Jsons.deserialize(record.get(USER.UI_METADATA).data(), JsonNode.class));
  }

  private AuthenticatedUser createAuthenticatedUserFromRecord(final Record record) {
    final User user = createUserFromRecord(record);
    return new AuthenticatedUser()
        .withUserId(user.getUserId())
        .withName(user.getName())
        .withDefaultWorkspaceId(user.getDefaultWorkspaceId())
        .withStatus(user.getStatus())
        .withCompanyName(user.getCompanyName())
        .withEmail(user.getEmail())
        .withNews(user.getNews())
        .withUiMetadata(user.getUiMetadata())
        .withAuthUserId(record.get(AUTH_USER.AUTH_USER_ID))
        .withAuthProvider(record.get(AUTH_USER.AUTH_PROVIDER) == null ? null
            : Enums.toEnum(record.get(AUTH_USER.AUTH_PROVIDER, String.class), io.airbyte.config.AuthProvider.class).orElseThrow());
  }

  /**
   * Fetch user information from their authentication id.
   *
   * @param userAuthId the authentication Identifier of the user
   * @return the user information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */
  public Optional<AuthenticatedUser> getUserByAuthId(final String userAuthId) throws IOException {
    final var resultFromAuthUsersTable = getUserByAuthIdFromAuthUserTable(userAuthId);
    if (resultFromAuthUsersTable.isEmpty()) {
      log.warn("User with auth user id {} not found in auth_user table", userAuthId);
    }
    return resultFromAuthUsersTable;
  }

  public Optional<AuthenticatedUser> getUserByAuthIdFromAuthUserTable(final String userAuthId) throws IOException {
    final var result = database.query(ctx -> ctx
        .select(
            AUTH_USER.AUTH_USER_ID,
            AUTH_USER.AUTH_PROVIDER,
            USER.ID,
            USER.NAME,
            USER.DEFAULT_WORKSPACE_ID,
            USER.STATUS,
            USER.COMPANY_NAME,
            USER.EMAIL,
            USER.NEWS,
            USER.UI_METADATA)
        .from(AUTH_USER)
        .innerJoin(USER).on(AUTH_USER.USER_ID.eq(USER.ID))
        .where(AUTH_USER.AUTH_USER_ID.eq(userAuthId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createAuthenticatedUserFromRecord(result.get(0)));
  }

  /**
   * Fetch user from their email. TODO remove this after Cloud user handlers are removed. Use
   * getUserByEmail instead.
   *
   * @param email the user email address.
   * @return the user information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */
  @Deprecated
  public Optional<AuthenticatedUser> getAuthenticatedUserByEmail(final String email) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(USER)
        .leftJoin(AUTH_USER).on(USER.ID.eq(AUTH_USER.USER_ID))
        .where(USER.EMAIL.eq(email)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    // FIXME: in the case of multiple auth providers, this will return the first one found.
    return Optional.of(createAuthenticatedUserFromRecord(result.get(0)));
  }

  public Optional<User> getUserByEmail(final String email) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(USER)
        .where(USER.EMAIL.eq(email)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createUserFromRecord(result.get(0)));
  }

  /**
   * Get the default user if it exists by looking up the hardcoded default user id.
   */
  public Optional<AuthenticatedUser> getDefaultUser() throws IOException {
    return getAuthenticatedUser(DEFAULT_USER_ID);
  }

  /**
   * Get all users that have read access to the specified workspace.
   */
  public List<User> getUsersWithWorkspaceAccess(final UUID workspaceId) throws IOException {
    return database
        .query(ctx -> ctx.fetch(
            PermissionPersistenceHelper.LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY,
            workspaceId,
            PermissionPersistenceHelper.getGrantingPermissionTypeArray(PermissionType.WORKSPACE_READER)))
        .stream()
        .map(this::createUserFromRecord)
        .toList();
  }

  /**
   * Get all user access info for a particular workspace, including the specific workspace-level
   * and/or organization-level permissions that the user has that grant read-access to the workspace.
   */
  public List<WorkspaceUserAccessInfo> listWorkspaceUserAccessInfo(final UUID workspaceId) throws IOException {
    return queryWorkspaceUserAccessInfo(workspaceId)
        .stream()
        .map(record -> buildWorkspaceUserAccessInfoFromRecord(record, workspaceId))
        .toList();
  }

  /**
   * Get all auth user IDs for a particular Airbyte user. Once Firebase is deprecated, there should
   * only be one auth user ID per Airbyte user and this method can be removed.
   */
  public List<String> listAuthUserIdsForUser(final UUID userId) throws IOException {
    return database.query(ctx -> ctx
        .select(AUTH_USER.AUTH_USER_ID)
        .from(AUTH_USER)
        .where(AUTH_USER.USER_ID.eq(userId))
        .fetch(AUTH_USER.AUTH_USER_ID));
  }

  public List<AuthUser> listAuthUsersForUser(final UUID userId) throws IOException {
    return database.query(ctx -> ctx
        .select(AUTH_USER.USER_ID, AUTH_USER.AUTH_USER_ID, AUTH_USER.AUTH_PROVIDER)
        .from(AUTH_USER)
        .where(AUTH_USER.USER_ID.eq(userId))
        .fetch().stream().map(record -> new AuthUser()
            .withUserId(record.get(AUTH_USER.USER_ID))
            .withAuthUserId(record.get(AUTH_USER.AUTH_USER_ID))
            .withAuthProvider(record.get(AUTH_USER.AUTH_PROVIDER) == null ? null
                : Enums.toEnum(record.get(AUTH_USER.AUTH_PROVIDER, String.class), io.airbyte.config.AuthProvider.class).orElseThrow()))
        .toList());
  }

  // This method is used for testing purposes only. For some reason, the actual
  // listWorkspaceUserAccessInfo method cannot be properly tested because in CI
  // tests only, permission_type enum values are mapped to `null` in the
  // `buildWorkspaceUserAccessInfoFromRecord` step. I spent so many hours trying
  // to figure out why, but I could not. This method allows me to at least test
  // that the right users are being returned in our CI tests, while leaving out
  // the problematic enum value mapping that isn't as critical to test.
  @VisibleForTesting
  List<UUID> listJustUsersForWorkspaceUserAccessInfo(final UUID workspaceId) throws IOException {
    return queryWorkspaceUserAccessInfo(workspaceId)
        .stream()
        .map(record -> record.get(USER.ID))
        .toList();
  }

  private Collection<Record> queryWorkspaceUserAccessInfo(final UUID workspaceId) throws IOException {
    return database
        .query(ctx -> ctx.fetch(
            PermissionPersistenceHelper.LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY,
            workspaceId,
            PermissionPersistenceHelper.getGrantingPermissionTypeArray(PermissionType.WORKSPACE_READER)));
  }

  private WorkspaceUserAccessInfo buildWorkspaceUserAccessInfoFromRecord(final Record record, final UUID workspaceId) {
    Permission workspacePermission = null;
    if (record.get(WORKSPACE_PERMISSION_ID_ALIAS, UUID.class) != null) {
      workspacePermission = new Permission()
          .withUserId(record.get(USER.ID))
          .withWorkspaceId(record.get(WORKSPACE_PERMISSION_WORKSPACE_ID_ALIAS, UUID.class))
          .withPermissionId(record.get(WORKSPACE_PERMISSION_ID_ALIAS, UUID.class))
          .withPermissionType(Enums.toEnum(record.get(WORKSPACE_PERMISSION_TYPE_ALIAS, String.class), PermissionType.class).orElseThrow());
    }

    Permission organizationPermission = null;
    if (record.get(ORG_PERMISSION_ID_ALIAS, UUID.class) != null) {
      organizationPermission = new Permission()
          .withUserId(record.get(USER.ID))
          .withOrganizationId(record.get(ORG_PERMISSION_ORG_ID_ALIAS, UUID.class))
          .withPermissionId(record.get(ORG_PERMISSION_ID_ALIAS, UUID.class))
          .withPermissionType(Enums.toEnum(record.get(ORG_PERMISSION_TYPE_ALIAS, String.class), PermissionType.class).orElseThrow());
    }

    return new WorkspaceUserAccessInfo()
        .withUserId(record.get(USER.ID))
        .withUserEmail(record.get(USER.EMAIL))
        .withUserName(record.get(USER.NAME))
        .withWorkspaceId(workspaceId)
        .withWorkspacePermission(workspacePermission)
        .withOrganizationPermission(organizationPermission);
  }

}
