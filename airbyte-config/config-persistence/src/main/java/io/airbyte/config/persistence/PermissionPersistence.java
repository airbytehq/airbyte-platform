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
import io.airbyte.config.UserPermission;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.sql.SQLException;
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
    final io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType permissionType =
        PermissionPersistenceHelper.convertConfigPermissionTypeToJooqPermissionType(permission.getPermissionType());

    database.transaction(ctx -> {
      final OffsetDateTime timestamp = OffsetDateTime.now();
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(PERMISSION)
          .where(PERMISSION.ID.eq(permission.getPermissionId())));

      if (isExistingConfig) {
        updatePermission(ctx, permission, timestamp);
      } else {
        ctx.insertInto(PERMISSION)
            .set(PERMISSION.ID, permission.getPermissionId())
            .set(PERMISSION.PERMISSION_TYPE, permissionType)
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

  private void updatePermission(final DSLContext transactionCtx, final Permission updatedPermission, final OffsetDateTime timestamp)
      throws SQLException {

    final Permission priorPermission;
    try {
      priorPermission = getPermission(updatedPermission.getPermissionId()).orElseThrow();
    } catch (IOException e) {
      throw new SQLException(e);
    }

    final io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType priorPermissionType =
        PermissionPersistenceHelper.convertConfigPermissionTypeToJooqPermissionType(priorPermission.getPermissionType());

    final io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType newPermissionType =
        PermissionPersistenceHelper.convertConfigPermissionTypeToJooqPermissionType(updatedPermission.getPermissionType());

    transactionCtx.update(PERMISSION)
        .set(PERMISSION.PERMISSION_TYPE, newPermissionType)
        .set(PERMISSION.WORKSPACE_ID, updatedPermission.getWorkspaceId())
        .set(PERMISSION.ORGANIZATION_ID, updatedPermission.getOrganizationId())
        .set(PERMISSION.USER_ID, updatedPermission.getUserId())
        .set(PERMISSION.UPDATED_AT, timestamp)
        .where(PERMISSION.ID.eq(updatedPermission.getPermissionId()))
        .execute();

    // if the updated permission started off as an organization admin, check to see if the org still
    // has an admin after the update is applied. if not, roll back the transaction.
    final boolean wasOrganizationAdminUpdate =
        priorPermissionType.equals(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.organization_admin);

    // use priorPermission instead of updatedPermission in case the organization ID changed in the
    // update.
    if (wasOrganizationAdminUpdate && countOrganizationAdmins(transactionCtx, priorPermission.getOrganizationId()) < 1) {
      // trigger a transaction rollback
      throw new SQLOperationNotAllowedException(
          "Preventing update that would have removed the last OrganizationAdmin from organization " + updatedPermission.getOrganizationId());
    }
  }

  private int countOrganizationAdmins(final DSLContext ctx, final UUID organizationId) {
    // fetch the count of permission records with type OrganizationAdmin and in the indicated
    // organizationId
    return ctx.fetchCount(select()
        .from(PERMISSION)
        .where(PERMISSION.PERMISSION_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.organization_admin))
        .and(PERMISSION.ORGANIZATION_ID.eq(organizationId)));
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
    return database.transaction(ctx -> {
      final Permission deletedPermission;
      try {
        deletedPermission = getPermission(permissionId).orElseThrow();
      } catch (final IOException e) {
        throw new SQLException(e);
      }
      final int modifiedCount = ctx.deleteFrom(PERMISSION).where(field(DSL.name(PRIMARY_KEY)).eq(permissionId)).execute();

      // return early if nothing was deleted
      if (modifiedCount == 0) {
        return false;
      }

      // check if this deletion removed the last OrganizationAdmin from the organization
      final boolean wasOrganizationAdminDeletion = deletedPermission.getPermissionType().equals(PermissionType.ORGANIZATION_ADMIN);
      if (wasOrganizationAdminDeletion && countOrganizationAdmins(ctx, deletedPermission.getOrganizationId()) < 1) {
        // trigger a rollback by throwing an exception
        throw new SQLOperationNotAllowedException(
            "Rolling back delete that would have removed the last OrganizationAdmin from organization " + deletedPermission.getOrganizationId());
      }

      return modifiedCount > 0;
    });
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

  public Boolean isUserInstanceAdmin(final UUID userId) throws IOException {
    return this.database.query(ctx -> isUserInstanceAdmin(ctx, userId));
  }

  private Boolean isUserInstanceAdmin(final DSLContext ctx, final UUID userId) {
    return ctx.fetchExists(select()
        .from(PERMISSION)
        .where(PERMISSION.PERMISSION_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.instance_admin))
        .and(PERMISSION.USER_ID.eq(userId)));
  }

  public Boolean isAuthUserInstanceAdmin(final String authUserId) throws IOException {
    return this.database.query(ctx -> isAuthUserInstanceAdmin(ctx, authUserId));
  }

  private Boolean isAuthUserInstanceAdmin(final DSLContext ctx, final String authUserId) {
    return ctx.fetchExists(select()
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.PERMISSION_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.instance_admin))
        .and(USER.AUTH_USER_ID.eq(authUserId)));
  }

  public PermissionType findPermissionTypeForUserAndWorkspace(final UUID workspaceId, final String authUserId)
      throws IOException {
    return this.database.query(ctx -> findPermissionTypeForUserAndWorkspace(ctx, workspaceId, authUserId));
  }

  private PermissionType findPermissionTypeForUserAndWorkspace(final DSLContext ctx,
                                                               final UUID workspaceId,
                                                               final String authUserId) {
    var record = ctx.select(PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.WORKSPACE_ID.eq(workspaceId))
        .and(USER.AUTH_USER_ID.eq(authUserId))
        .fetchOne();
    if (record == null) {
      return null;
    }

    final var jooqPermissionType = record.get(PERMISSION.PERMISSION_TYPE, io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.class);

    return Enums.toEnum(jooqPermissionType.getLiteral(), PermissionType.class).get();
  }

  public PermissionType findPermissionTypeForUserAndOrganization(final UUID organizationId, final String authUserId)
      throws IOException {
    return this.database.query(ctx -> findPermissionTypeForUserAndOrganization(ctx, organizationId, authUserId));
  }

  private PermissionType findPermissionTypeForUserAndOrganization(final DSLContext ctx,
                                                                  final UUID organizationId,
                                                                  final String authUserId) {
    var record = ctx.select(PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .and(USER.AUTH_USER_ID.eq(authUserId))
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

  /**
   * List all organization-level permissions for an organization.
   */
  public List<UserPermission> listPermissionsForOrganization(final UUID organizationId) throws IOException {
    return this.database.query(ctx -> listPermissionsForOrganization(ctx, organizationId));
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
