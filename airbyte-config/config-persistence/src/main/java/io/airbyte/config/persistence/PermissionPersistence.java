/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.PERMISSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.USER;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;

import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.UserPermission;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

/**
 * Permission Persistence.
 *
 * Handle persisting Permission to the Config Database and perform all SQL queries.
 *
 */
public class PermissionPersistence {

  private final ExceptionWrappingDatabase database;

  public static final String PRIMARY_KEY = "id";
  public static final String USER_KEY = "user_id";
  public static final String WORKSPACE_KEY = "workspace_id";

  public PermissionPersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Create or update Permission.
   *
   * @param permission permission to write into database.
   * @throws IOException in case of a db error.
   */

  public void writePermission(final Permission permission) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    database.transaction(ctx -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(PERMISSION)
          .where(PERMISSION.ID.eq(permission.getPermissionId())));

      if (isExistingConfig) {
        ctx.update(PERMISSION)
            .set(PERMISSION.ID, permission.getPermissionId())
            .set(PERMISSION.PERMISSION_TYPE, permission.getPermissionType() == null ? null
                : Enums.toEnum(permission.getPermissionType().value(),
                    io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.class).orElseThrow())
            .set(PERMISSION.USER_ID, permission.getUserId())
            .set(PERMISSION.WORKSPACE_ID, permission.getWorkspaceId())
            .set(PERMISSION.ORGANIZATION_ID, permission.getOrganizationId())
            .set(PERMISSION.UPDATED_AT, timestamp)
            .where(PERMISSION.ID.eq(permission.getPermissionId()))
            .execute();

      } else {
        ctx.insertInto(PERMISSION)
            .set(PERMISSION.ID, permission.getPermissionId())
            .set(PERMISSION.PERMISSION_TYPE, permission.getPermissionType() == null ? null
                : Enums.toEnum(permission.getPermissionType().value(),
                    io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.class).orElseThrow())
            .set(PERMISSION.USER_ID, permission.getUserId())
            .set(PERMISSION.WORKSPACE_ID, permission.getWorkspaceId())
            .set(PERMISSION.ORGANIZATION_ID, permission.getOrganizationId())
            .set(PERMISSION.CREATED_AT, timestamp)
            .set(PERMISSION.UPDATED_AT, timestamp)
            .execute();
      }
      return null;
    });
  }

  /**
   * Get a permission by permission Id.
   *
   * @param permissionId the permission id
   * @return the permission information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */

  public Optional<Permission> getPermission(final UUID permissionId) throws IOException {

    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(PERMISSION)
        .where(PERMISSION.ID.eq(permissionId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createPermissionFromRecord(result.get(0)));
  }

  /**
   * List permissions by User id.
   *
   * @param userId the user id
   * @return list of permissions associate with the user
   * @throws IOException in case of a db error
   */
  public List<Permission> listPermissionsByUser(final UUID userId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(PERMISSION)
        .where(PERMISSION.USER_ID.eq(userId))
        .fetch());
    return result.stream().map(this::createPermissionFromRecord).collect(Collectors.toList());
  }

  /**
   * List permissions by workspace id.
   *
   * @param workspaceId the workspace id
   * @return list of permissions associate with given workspace
   * @throws IOException in case of a db error
   */
  public List<Permission> listPermissionByWorkspace(final UUID workspaceId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(PERMISSION)
        .where(PERMISSION.WORKSPACE_ID.eq(workspaceId))
        .fetch());
    return result.stream().map(this::createPermissionFromRecord).collect(Collectors.toList());
  }

  private Permission createPermissionFromRecord(final Record record) {
    return new Permission()
        .withPermissionId(record.get(PERMISSION.ID))
        .withPermissionType(record.get(PERMISSION.PERMISSION_TYPE) == null ? null
            : Enums.toEnum(record.get(PERMISSION.PERMISSION_TYPE, String.class), PermissionType.class).orElseThrow())
        .withUserId(record.get(PERMISSION.USER_ID))
        .withWorkspaceId(record.get(PERMISSION.WORKSPACE_ID))
        .withOrganizationId(record.get(PERMISSION.ORGANIZATION_ID));
  }

  /**
   * Delete Permissions by id.
   *
   *
   */
  public boolean deletePermissionById(final UUID permissionId) throws IOException {
    return database.transaction(ctx -> ctx.deleteFrom(PERMISSION)).where(field(DSL.name(PRIMARY_KEY)).eq(permissionId)).execute() > 0;
  }

  /**
   * Delete Permissions by User id.
   *
   * @param userId the user id
   * @throws IOException in case of a db error
   */
  public boolean deletePermissionByUserId(final UUID userId) throws IOException {
    return database.transaction(ctx -> ctx.deleteFrom(PERMISSION)).where(field(DSL.name(USER_KEY)).eq(userId)).execute() > 0;
  }

  /**
   * Delete Permissions by workspace.
   *
   * @param workspaceId the workspace id
   * @throws IOException in case of a db error
   */
  public boolean deletePermissionByWorkspaceId(final UUID workspaceId) throws IOException {
    return database.transaction(ctx -> ctx.deleteFrom(PERMISSION)).where(field(DSL.name(WORKSPACE_KEY)).eq(workspaceId)).execute() > 0;
  }

  /**
   * List all users with permissions to the workspace. Note it does not take organization info into
   * account.
   *
   * @param workspaceId workspace id
   * @return all users with their own permission type to the workspace. The list will not include org
   *         users unless they are specifically permissioned as a workspace user.
   * @throws IOException if there is an issue while interacting with the db.
   */
  public List<UserPermission> listUsersInWorkspace(final UUID workspaceId) throws IOException {
    return this.database.query(ctx -> listPermissionsForWorkspace(ctx, workspaceId));
  }

  public List<UserPermission> listInstanceAdminUsers() throws IOException {
    return this.database.query(ctx -> listInstanceAdminPermissions(ctx));
  }

  public List<UserPermission> listUsersInOrganization(final UUID organizationId) throws IOException {
    return this.database.query(ctx -> listPermissionsForOrganization(ctx, organizationId));
  }

  private List<UserPermission> listInstanceAdminPermissions(final DSLContext ctx) {
    var records = ctx.select(USER.ID, USER.NAME, USER.EMAIL, USER.DEFAULT_WORKSPACE_ID, PERMISSION.ID, PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.PERMISSION_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.instance_admin))
        .fetch();

    return records.stream().map(record -> buildUserPermissionFromRecord(record)).collect(Collectors.toList());
  }

  private UserPermission getUserInstanceAdminPermission(final DSLContext ctx, final UUID userId) {
    var record = ctx.select(USER.ID, USER.NAME, USER.EMAIL, USER.DEFAULT_WORKSPACE_ID, PERMISSION.ID, PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.PERMISSION_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.instance_admin))
        .and(PERMISSION.USER_ID.eq(userId))
        .fetchOne();
    if (record == null) {
      return null;
    }
    return buildUserPermissionFromRecord(record);
  }

  /**
   * Check and get instance_admin permission for a user.
   *
   * @param userId user id
   * @return UserPermission User details with instance_admin permission, null if user does not have
   *         instance_admin role.
   * @throws IOException if there is an issue while interacting with the db.
   */
  public UserPermission getUserInstanceAdminPermission(final UUID userId) throws IOException {
    return this.database.query(ctx -> getUserInstanceAdminPermission(ctx, userId));
  }

  public PermissionType findPermissionTypeForUserAndWorkspace(final UUID workspaceId, final String authUserId, final AuthProvider authProvider)
      throws IOException {
    return this.database.query(ctx -> findPermissionTypeForUserAndWorkspace(ctx, workspaceId, authUserId, authProvider));
  }

  private PermissionType findPermissionTypeForUserAndWorkspace(final DSLContext ctx,
                                                               final UUID workspaceId,
                                                               final String authUserId,
                                                               final AuthProvider authProvider) {
    var record = ctx.select(PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.WORKSPACE_ID.eq(workspaceId))
        .and(USER.AUTH_USER_ID.eq(authUserId))
        .and(USER.AUTH_PROVIDER.eq(Enums.toEnum(authProvider.value(), io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider.class).get()))
        .fetchOne();
    if (record == null) {
      return null;
    }

    final var jooqPermissionType = record.get(PERMISSION.PERMISSION_TYPE, io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.class);

    return Enums.toEnum(jooqPermissionType.getLiteral(), PermissionType.class).get();
  }

  public PermissionType findPermissionTypeForUserAndOrganization(final UUID organizationId, final String authUserId, final AuthProvider authProvider)
      throws IOException {
    return this.database.query(ctx -> findPermissionTypeForUserAndOrganization(ctx, organizationId, authUserId, authProvider));
  }

  private PermissionType findPermissionTypeForUserAndOrganization(final DSLContext ctx,
                                                                  final UUID organizationId,
                                                                  final String authUserId,
                                                                  final AuthProvider authProvider) {
    var record = ctx.select(PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .and(USER.AUTH_USER_ID.eq(authUserId))
        .and(USER.AUTH_PROVIDER.eq(Enums.toEnum(authProvider.value(), io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider.class).get()))
        .fetchOne();

    if (record == null) {
      return null;
    }

    final var jooqPermissionType = record.get(PERMISSION.PERMISSION_TYPE, io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.class);
    return Enums.toEnum(jooqPermissionType.getLiteral(), PermissionType.class).get();
  }

  private List<UserPermission> listPermissionsForWorkspace(final DSLContext ctx, final UUID workspaceId) {
    var records = ctx.select(USER.ID, USER.NAME, USER.EMAIL, USER.DEFAULT_WORKSPACE_ID, PERMISSION.ID, PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.WORKSPACE_ID.eq(workspaceId))
        .fetch();

    return records.stream().map(record -> buildUserPermissionFromRecord(record)).collect(Collectors.toList());
  }

  private List<UserPermission> listPermissionsForOrganization(final DSLContext ctx, final UUID organizationId) {
    var records = ctx.select(USER.ID, USER.NAME, USER.EMAIL, USER.DEFAULT_WORKSPACE_ID, PERMISSION.ID, PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .fetch();

    return records.stream().map(record -> buildUserPermissionFromRecord(record)).collect(Collectors.toList());
  }

  private UserPermission buildUserPermissionFromRecord(final Record record) {
    return new UserPermission()
        .withUser(
            new User()
                .withUserId(record.get(USER.ID))
                .withName(record.get(USER.NAME))
                .withEmail(record.get(USER.EMAIL))
                .withDefaultWorkspaceId(record.get(USER.DEFAULT_WORKSPACE_ID)))
        .withPermission(
            new Permission()
                .withPermissionId(record.get(PERMISSION.ID))
                .withPermissionType(Enums.toEnum(record.get(PERMISSION.PERMISSION_TYPE).toString(), PermissionType.class).get()));
  }

}
