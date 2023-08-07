/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.PERMISSION;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;

import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
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
        .withWorkspaceId(record.get(PERMISSION.WORKSPACE_ID));
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

}
