/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.field;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: update migration description in the class name
public class V0_64_4_002__AddJobRunnerPermissionTypes extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_64_4_002__AddJobRunnerPermissionTypes.class);
  private static final String PERMISSION_TYPE = "permission_type";
  private static final String ORGANIZATION_ID = "organization_id";
  private static final String WORKSPACE_ID = "workspace_id";
  private static final Table<Record> PERMISSION_TABLE = DSL.table("permission");
  private static final String PERMISSION_CONSTRAINT_NAME = "permission_check_organization_id_and_workspace_id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    runMigration(ctx);
  }

  public static void runMigration(DSLContext ctx) {
    alterEnum(ctx);
    editPermissionConstraint(ctx);
  }

  private static void alterEnum(DSLContext ctx) {
    ctx.transaction(configuration -> {
      // SO we can do testing more easily
      ctx.execute("ALTER TYPE permission_type ADD VALUE IF NOT EXISTS 'organization_runner'");
      ctx.execute("ALTER TYPE permission_type ADD VALUE IF NOT EXISTS 'workspace_runner'");
    });
  }

  private static void editPermissionConstraint(final DSLContext ctx) {
    ctx.transaction(configuration -> {
      ctx.alterTable(PERMISSION_TABLE).dropConstraintIfExists(PERMISSION_CONSTRAINT_NAME).execute();
      ctx.alterTable(PERMISSION_TABLE)
          .add(constraint(PERMISSION_CONSTRAINT_NAME).check(
              field(PERMISSION_TYPE).eq("instance_admin").and(field(WORKSPACE_ID).isNull()).and(field(ORGANIZATION_ID).isNull())
                  .or(
                      field(PERMISSION_TYPE).in("organization_admin", "organization_editor", "organization_reader", "organization_member",
                          "organization_runner")
                          .and(field(WORKSPACE_ID).isNull()).and(field(ORGANIZATION_ID).isNotNull()))
                  .or(
                      field(PERMISSION_TYPE).in("workspace_admin", "workspace_editor", "workspace_reader", "workspace_runner")
                          .and(field(WORKSPACE_ID).isNotNull())
                          .and(field(ORGANIZATION_ID).isNull()))))
          .execute();
    });

  }

}
