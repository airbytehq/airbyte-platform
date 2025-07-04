/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.AUTH_USER;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.PERMISSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.USER;

import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.config.UserPermission;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.Record;

/**
 * Permission Persistence.
 *
 * Handle persisting Permission to the Config Database and perform all SQL queries.
 *
 * @deprecated to be replaced by {@link io.airbyte.data.repositories.PermissionRepository}
 */
@Deprecated
public class PermissionPersistence {

  private final ExceptionWrappingDatabase database;

  public PermissionPersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  public List<UserPermission> listInstanceAdminUsers() throws IOException {
    return this.database.query(this::listInstanceAdminPermissions);
  }

  public List<UserPermission> listUsersInOrganization(final UUID organizationId) throws IOException {
    return this.database.query(ctx -> listPermissionsForOrganization(ctx, organizationId));
  }

  private List<UserPermission> listInstanceAdminPermissions(final DSLContext ctx) {
    final var records = ctx.select(USER.ID, USER.NAME, USER.EMAIL, USER.DEFAULT_WORKSPACE_ID, PERMISSION.ID, PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.PERMISSION_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.instance_admin))
        .fetch();

    return records.stream().map(this::buildUserPermissionFromRecord).collect(Collectors.toList());
  }

  public PermissionType findPermissionTypeForUserAndWorkspace(final UUID workspaceId, final String authUserId)
      throws IOException {
    return this.database.query(ctx -> findPermissionTypeForUserAndWorkspace(ctx, workspaceId, authUserId));
  }

  private PermissionType findPermissionTypeForUserAndWorkspace(final DSLContext ctx,
                                                               final UUID workspaceId,
                                                               final String authUserId) {
    final var record = ctx.select(PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .join(AUTH_USER)
        .on(USER.ID.eq(AUTH_USER.USER_ID))
        .where(PERMISSION.WORKSPACE_ID.eq(workspaceId))
        .and(AUTH_USER.AUTH_USER_ID.eq(authUserId))
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
    final var record = ctx.select(PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .join(AUTH_USER)
        .on(USER.ID.eq(AUTH_USER.USER_ID))
        .where(PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .and(AUTH_USER.AUTH_USER_ID.eq(authUserId))
        .fetchOne();

    if (record == null) {
      return null;
    }

    final var jooqPermissionType = record.get(PERMISSION.PERMISSION_TYPE, io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.class);
    return Enums.toEnum(jooqPermissionType.getLiteral(), PermissionType.class).get();
  }

  /**
   * List all organization-level permissions for an organization.
   */
  public List<UserPermission> listPermissionsForOrganization(final UUID organizationId) throws IOException {
    return this.database.query(ctx -> listPermissionsForOrganization(ctx, organizationId));
  }

  private List<UserPermission> listPermissionsForOrganization(final DSLContext ctx, final UUID organizationId) {
    final var records = ctx.select(USER.ID, USER.NAME, USER.EMAIL, USER.DEFAULT_WORKSPACE_ID, PERMISSION.ID, PERMISSION.PERMISSION_TYPE)
        .from(PERMISSION)
        .join(USER)
        .on(PERMISSION.USER_ID.eq(USER.ID))
        .where(PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .fetch();

    return records.stream().map(this::buildUserPermissionFromRecord).collect(Collectors.toList());
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
