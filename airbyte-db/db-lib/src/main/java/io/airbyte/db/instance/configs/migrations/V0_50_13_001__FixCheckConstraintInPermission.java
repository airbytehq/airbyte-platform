/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.field;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This new migration is to fix a check constraint in a previous migration:
 * V0_50_11_002__AddOrganizationIdColumnToPermission.
 */
public class V0_50_13_001__FixCheckConstraintInPermission extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_13_001__FixCheckConstraintInPermission.class);
  private static final String PERMISSION_TABLE = "permission";
  private static final String ORGANIZATION_ID_COLUMN = "organization_id";
  private static final String WORKSPACE_ID_COLUMN = "workspace_id";
  private static final String CHECK_CONSTRAINT_NAME = "permission_check_access_type";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    LOGGER.info("Fix check constraint in Permission table...");
    fixCheckConstraint(ctx);
    LOGGER.info("Migration finished!");
  }

  private static void fixCheckConstraint(final DSLContext ctx) {
    ctx.alterTable(PERMISSION_TABLE)
        .dropConstraintIfExists(CHECK_CONSTRAINT_NAME)
        .execute();
    ctx.alterTable(PERMISSION_TABLE)
        .add(constraint(CHECK_CONSTRAINT_NAME).check(
            field(WORKSPACE_ID_COLUMN).isNull().or(field(ORGANIZATION_ID_COLUMN).isNull())))
        .execute();
  }

}
