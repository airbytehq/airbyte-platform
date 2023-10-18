/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.USER;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;

import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider;
import io.airbyte.db.instance.configs.jooq.generated.enums.Status;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

/**
 * User Persistence.
 *
 * Perform all SQL queries and handle persisting User to the Config Database.
 *
 */
@Slf4j
public class UserPersistence {

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
   */
  public void writeUser(final User user) throws IOException {
    database.transaction(ctx -> {
      final OffsetDateTime timestamp = OffsetDateTime.now();
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(USER)
          .where(USER.ID.eq(user.getUserId())));

      if (isExistingConfig) {
        ctx.update(USER)
            .set(USER.ID, user.getUserId())
            .set(USER.NAME, user.getName())
            .set(USER.AUTH_USER_ID, user.getAuthUserId())
            .set(USER.AUTH_PROVIDER, user.getAuthProvider() == null ? null
                : Enums.toEnum(user.getAuthProvider().value(), AuthProvider.class).orElseThrow())
            .set(USER.DEFAULT_WORKSPACE_ID, user.getDefaultWorkspaceId())
            .set(USER.STATUS, user.getStatus() == null ? null
                : Enums.toEnum(user.getStatus().value(), Status.class).orElseThrow())
            .set(USER.COMPANY_NAME, user.getCompanyName())
            .set(USER.EMAIL, user.getEmail())
            .set(USER.NEWS, user.getNews())
            .set(USER.UPDATED_AT, timestamp)
            .where(USER.ID.eq(user.getUserId()))
            .execute();

      } else {
        ctx.insertInto(USER)
            .set(USER.ID, user.getUserId())
            .set(USER.NAME, user.getName())
            .set(USER.AUTH_USER_ID, user.getAuthUserId())
            .set(USER.AUTH_PROVIDER, user.getAuthProvider() == null ? null
                : Enums.toEnum(user.getAuthProvider().value(), AuthProvider.class).orElseThrow())
            .set(USER.DEFAULT_WORKSPACE_ID, user.getDefaultWorkspaceId())
            .set(USER.STATUS, user.getStatus() == null ? null
                : Enums.toEnum(user.getStatus().value(), Status.class).orElseThrow())
            .set(USER.COMPANY_NAME, user.getCompanyName())
            .set(USER.EMAIL, user.getEmail())
            .set(USER.NEWS, user.getNews())
            .set(USER.CREATED_AT, timestamp)
            .set(USER.UPDATED_AT, timestamp)
            .execute();
      }
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
        .withAuthUserId(record.get(USER.AUTH_USER_ID))
        .withAuthProvider(record.get(USER.AUTH_PROVIDER) == null ? null
            : Enums.toEnum(record.get(USER.AUTH_PROVIDER, String.class), User.AuthProvider.class).orElseThrow())
        .withDefaultWorkspaceId(record.get(USER.DEFAULT_WORKSPACE_ID))
        .withStatus(record.get(USER.STATUS) == null ? null : Enums.toEnum(record.get(USER.STATUS, String.class), User.Status.class).orElseThrow())
        .withCompanyName(record.get(USER.COMPANY_NAME))
        .withEmail(record.get(USER.EMAIL))
        .withNews(record.get(USER.NEWS));
  }

  /**
   * Fetch user information from their authentication id.
   *
   * @param userAuthId the authentication Identifier of the user
   * @return the user information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */
  public Optional<User> getUserByAuthId(final String userAuthId) throws IOException {

    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(USER)
        .where(USER.AUTH_USER_ID.eq(userAuthId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createUserFromRecord(result.get(0)));

  }

  /**
   * Fetch user information from their email.
   *
   * @param email the user email address.
   * @return the user information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */
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
  public Optional<User> getDefaultUser() throws IOException {
    return getUser(DEFAULT_USER_ID);
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

}
