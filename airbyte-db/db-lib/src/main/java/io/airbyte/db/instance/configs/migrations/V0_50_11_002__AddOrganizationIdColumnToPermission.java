/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.field;

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
 * Add resource_type column to Permission table migration.
 */
public class V0_50_11_002__AddOrganizationIdColumnToPermission extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_11_002__AddOrganizationIdColumnToPermission.class);
  private static final String PERMISSION_TABLE = "permission";
  private static final String ORGANIZATION_TABLE = "organization";
  private static final String ORGANIZATION_ID_COLUMN = "organization_id";
  private static final String WORKSPACE_ID_COLUMN = "organization_id";
  private static final String ORGANIZATION_ID_FOREIGN_KEY = "permission_organization_id_fkey";
  private static final String ORGANIZATION_ID_INDEX = "permission_organization_id_idx";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    LOGGER.info("Add column organization_id to Permission table...");
    addOrganizationIdColumnToPermission(ctx);
    LOGGER.info("Migration finished!");
  }

  private static void addOrganizationIdColumnToPermission(final DSLContext ctx) {
    final Field<UUID> organizationId = DSL.field(ORGANIZATION_ID_COLUMN, SQLDataType.UUID.nullable(true));
    // 1. Add new column.
    ctx.alterTable(PERMISSION_TABLE)
        .addColumnIfNotExists(organizationId)
        .execute();
    // 2.1 Add foreign key constraint.
    ctx.alterTable(PERMISSION_TABLE)
        .add(constraint(ORGANIZATION_ID_FOREIGN_KEY).foreignKey(organizationId)
            .references(ORGANIZATION_TABLE, "id").onDeleteCascade())
        .execute();
    // 2.2 Add constraint check on access type: workspace OR organization.
    ctx.alterTable(PERMISSION_TABLE)
        .add(constraint("permission_check_access_type").check(
            field(WORKSPACE_ID_COLUMN).isNull().or(field(ORGANIZATION_ID_COLUMN).isNull())))
        .execute();
    // 3. Add new index.
    ctx.createIndexIfNotExists(ORGANIZATION_ID_INDEX)
        .on(PERMISSION_TABLE, ORGANIZATION_ID_COLUMN)
        .execute();
  }

}
