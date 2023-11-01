/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.field;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_24_009__AddConstraintInPermissionTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_009__AddConstraintInPermissionTable.class);
  private static final Table PERMISSION_TABLE = DSL.table("permission");
  private static final String ORGANIZATION_ID = "organization_id";
  private static final String WORKSPACE_ID = "workspace_id";
  private static final String PERMISSION_TYPE = "permission_type";
  private static final String PERMISSION_CONSTRAINT_NAME = "permission_check_organization_id_and_workspace_id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    addPermissionConstraint(ctx);
  }

  private static void addPermissionConstraint(final DSLContext ctx) {
    ctx.alterTable(PERMISSION_TABLE)
        .add(constraint(PERMISSION_CONSTRAINT_NAME).check(
            field(PERMISSION_TYPE).eq("instance_admin").and(field(WORKSPACE_ID).isNull()).and(field(ORGANIZATION_ID).isNull())
                .or(
                    field(PERMISSION_TYPE).in("organization_admin", "organization_editor", "organization_reader", "organization_member")
                        .and(field(WORKSPACE_ID).isNull()).and(field(ORGANIZATION_ID).isNotNull()))
                .or(
                    field(PERMISSION_TYPE).in("workspace_admin", "workspace_editor", "workspace_reader").and(field(WORKSPACE_ID).isNotNull())
                        .and(field(ORGANIZATION_ID).isNull()))))
        .execute();
  }

}
