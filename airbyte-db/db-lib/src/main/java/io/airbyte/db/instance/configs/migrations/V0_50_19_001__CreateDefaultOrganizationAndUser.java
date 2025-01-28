/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.select;

import io.airbyte.db.instance.configs.migrations.V0_44_4_001__AddUserAndPermissionTables.Status;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType;
import java.util.Optional;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This migration ensures that every instance of Airbyte has a default organization and user. For
 * instances that have already been set up with an email address persisted on the default workspace,
 * this migration will copy that email address to the default organization and user. Otherwise,
 * email will be blank.
 */
public class V0_50_19_001__CreateDefaultOrganizationAndUser extends BaseJavaMigration {

  private static final String WORKSPACE_TABLE = "workspace";
  // The user table is quoted to avoid conflict with the reserved user keyword in Postgres.
  private static final String USER_TABLE = "\"user\"";
  private static final String ORGANIZATION_TABLE = "organization";
  private static final String PERMISSION_TABLE = "permission";

  // Default values
  private static final AuthProvider DEFAULT_AUTH_PROVIDER = AuthProvider.AIRBYTE;
  private static final String DEFAULT_USER_NAME = "Default User";
  private static final Status DEFAULT_USER_STATUS = Status.REGISTERED;
  private static final String DEFAULT_ORGANIZATION_NAME = "Default Organization";
  // The all-zero UUID is used to reliably identify the default organization and user.
  private static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  // The default email address is blank, since it is a non-nullable column.
  private static final String DEFAULT_EMAIL = "";

  // Shared fields
  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
  private static final Field<String> EMAIL_COLUMN = DSL.field("email", SQLDataType.VARCHAR(256));
  private static final Field<String> NAME_COLUMN = DSL.field("name", SQLDataType.VARCHAR(256));
  private static final Field<UUID> USER_ID_COLUMN = DSL.field("user_id", SQLDataType.UUID);

  // Workspace specific fields
  private static final Field<Boolean> INITIAL_SETUP_COMPLETE_COLUMN = DSL.field("initial_setup_complete", SQLDataType.BOOLEAN);
  private static final Field<Boolean> TOMBSTONE_COLUMN = DSL.field("tombstone", SQLDataType.BOOLEAN);
  private static final Field<UUID> ORGANIZATION_ID_COLUMN = DSL.field("organization_id", SQLDataType.UUID);

  // User specific fields
  private static final Field<String> AUTH_USER_ID_COLUMN = DSL.field("auth_user_id", SQLDataType.VARCHAR(256));
  private static final Field<UUID> DEFAULT_WORKSPACE_ID_COLUMN = DSL.field("default_workspace_id", SQLDataType.UUID);
  private static final Field<Status> STATUS_COLUMN = DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(Status.class));
  private static final Field<AuthProvider> AUTH_PROVIDER_COLUMN = DSL.field("auth_provider", SQLDataType.VARCHAR.asEnumDataType(AuthProvider.class));

  // Permission specific fields
  private static final Field<PermissionType> PERMISSION_TYPE_COLUMN =
      DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class));

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_19_001__CreateDefaultOrganizationAndUser.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createDefaultUserAndOrganization(ctx);
  }

  static void createDefaultUserAndOrganization(final DSLContext ctx) {
    // return early if a default user or default organization already exist.
    // this shouldn't happen in practice, but if this migration somehow gets run
    // multiple times or an instance is for some reason already using the
    // all-zero UUID, we don't want to overwrite any existing records.
    if (ctx.fetchExists(select()
        .from(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID)))) {
      LOGGER.info("Default user already exists. Skipping this migration.");
      return;
    }

    if (ctx.fetchExists(select()
        .from(DSL.table(ORGANIZATION_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_ORGANIZATION_ID)))) {
      LOGGER.info("Default organization already exists. Skipping this migration.");
      return;
    }

    final Optional<UUID> workspaceIdOptional = getDefaultWorkspaceIdOptional(ctx);
    final String email = workspaceIdOptional.flatMap(workspaceId -> getWorkspaceEmailOptional(ctx, workspaceId)).orElse(DEFAULT_EMAIL);
    final UUID defaultWorkspaceId = workspaceIdOptional.orElse(null);

    // insert the default User record
    ctx.insertInto(DSL.table(USER_TABLE))
        .columns(ID_COLUMN, EMAIL_COLUMN, NAME_COLUMN, AUTH_USER_ID_COLUMN, DEFAULT_WORKSPACE_ID_COLUMN, STATUS_COLUMN, AUTH_PROVIDER_COLUMN)
        .values(DEFAULT_USER_ID, email, DEFAULT_USER_NAME, DEFAULT_USER_ID.toString(), defaultWorkspaceId, DEFAULT_USER_STATUS, DEFAULT_AUTH_PROVIDER)
        .execute();

    ctx.insertInto(DSL.table(ORGANIZATION_TABLE))
        .columns(ID_COLUMN, EMAIL_COLUMN, NAME_COLUMN, USER_ID_COLUMN)
        .values(DEFAULT_ORGANIZATION_ID, email, DEFAULT_ORGANIZATION_NAME, DEFAULT_USER_ID)
        .execute();

    // update the default workspace to point to the default organization
    if (workspaceIdOptional.isPresent()) {
      LOGGER.info("Updating default workspace with ID {} to belong to default organization with ID {}", workspaceIdOptional.get(),
          DEFAULT_ORGANIZATION_ID);
      ctx.update(DSL.table(WORKSPACE_TABLE))
          .set(ORGANIZATION_ID_COLUMN, DEFAULT_ORGANIZATION_ID)
          .where(ID_COLUMN.eq(workspaceIdOptional.get()))
          .execute();
    } else {
      LOGGER.info("No default workspace found. Skipping update of default workspace to point to default organization.");
    }

    // grant the default user admin permissions on the default organization
    LOGGER.info("Granting ORGANIZATION_ADMIN permission to default user with ID {} on default organization with ID {}", DEFAULT_USER_ID,
        DEFAULT_ORGANIZATION_ID);
    ctx.insertInto(DSL.table(PERMISSION_TABLE))
        .columns(ID_COLUMN, USER_ID_COLUMN, ORGANIZATION_ID_COLUMN, PERMISSION_TYPE_COLUMN)
        .values(UUID.randomUUID(), DEFAULT_USER_ID, DEFAULT_ORGANIZATION_ID, PermissionType.ORGANIZATION_ADMIN)
        .execute();
  }

  // Return the first non-tombstoned workspace in the instance with `initialSetupComplete: true`, if
  // it exists. Otherwise, just return the first workspace in the instance regardless of its setup
  // status, if it exists.
  private static Optional<UUID> getDefaultWorkspaceIdOptional(final DSLContext ctx) {
    final Optional<UUID> setupWorkspaceIdOptional = ctx.select(ID_COLUMN)
        .from(WORKSPACE_TABLE)
        .where(INITIAL_SETUP_COMPLETE_COLUMN.eq(true))
        .and(TOMBSTONE_COLUMN.eq(false))
        .limit(1)
        .fetchOptional(ID_COLUMN);

    // return the optional ID if it is present. Otherwise, return the first non-tombstoned workspace ID
    // in the database.
    return setupWorkspaceIdOptional.isPresent() ? setupWorkspaceIdOptional
        : ctx.select(ID_COLUMN)
            .from(WORKSPACE_TABLE)
            .where(TOMBSTONE_COLUMN.eq(false))
            .limit(1)
            .fetchOptional(ID_COLUMN);
  }

  // Find the email address of the default workspace, if it exists.
  private static Optional<String> getWorkspaceEmailOptional(final DSLContext ctx, final UUID workspaceId) {
    return ctx.select(EMAIL_COLUMN)
        .from(WORKSPACE_TABLE)
        .where(ID_COLUMN.eq(workspaceId))
        .fetchOptional(EMAIL_COLUMN);
  }

}
