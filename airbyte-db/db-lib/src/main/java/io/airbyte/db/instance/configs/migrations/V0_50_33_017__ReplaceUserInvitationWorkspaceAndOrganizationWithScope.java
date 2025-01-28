/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE;

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
 * Replace the UserInvitation workspace_id and organization_id columns with a scope_type and
 * scope_id column. Note that this table is not yet written to, so this migration does not need to
 * handle any data migration.
 */
public class V0_50_33_017__ReplaceUserInvitationWorkspaceAndOrganizationWithScope extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_017__ReplaceUserInvitationWorkspaceAndOrganizationWithScope.class);
  private static final String SCOPE_TYPE = "scope_type";
  private static final String SCOPE_ID = "scope_id";
  private static final Field<V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum> SCOPE_TYPE_COLUMN = DSL.field(SCOPE_TYPE,
      SQLDataType.VARCHAR.asEnumDataType(V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum.class).nullable(false));
  private static final Field<UUID> SCOPE_ID_COLUMN = DSL.field(SCOPE_ID, SQLDataType.UUID.nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    dropWorkspaceIdColumn(ctx);
    dropOrganizationIdColumn(ctx);
    addScopeIdAndScopeTypeColumns(ctx);
    addScopeTypeAndScopeIdIndexes(ctx);
  }

  static void dropWorkspaceIdColumn(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE)
        .dropColumn("workspace_id")
        .execute();
  }

  static void dropOrganizationIdColumn(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE)
        .dropColumn("organization_id")
        .execute();
  }

  static void addScopeIdAndScopeTypeColumns(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE).addColumnIfNotExists(SCOPE_ID_COLUMN).execute();
    ctx.alterTable(USER_INVITATION_TABLE).addColumnIfNotExists(SCOPE_TYPE_COLUMN).execute();
  }

  static void addScopeTypeAndScopeIdIndexes(final DSLContext ctx) {
    ctx.createIndex("user_invitation_scope_type_and_scope_id_index")
        .on(USER_INVITATION_TABLE, SCOPE_TYPE, SCOPE_ID)
        .execute();
    ctx.createIndex("user_invitation_scope_id_index")
        .on(USER_INVITATION_TABLE, SCOPE_ID)
        .execute();
  }

}
