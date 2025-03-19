/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.foreignKey;

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
 * Inserts an organization_id column to the workspace table. The organization_id is a foreign key to
 * the id of the organization table.
 */
public class V0_50_5_002__AddOrganizationColumnToWorkspaceTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_5_002__AddOrganizationColumnToWorkspaceTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addOrganizationColumnToWorkspace(ctx);
  }

  static void addOrganizationColumnToWorkspace(final DSLContext ctx) {
    final Field<UUID> organizationId = DSL.field("organization_id", SQLDataType.UUID.nullable(true));

    ctx.alterTable("workspace").addColumnIfNotExists(organizationId).execute();
    ctx.alterTable("workspace").add(foreignKey(organizationId).references("organization")).execute();

    LOGGER.info("organization_id column added to workspace table");
  }

}
