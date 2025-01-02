/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;

import com.google.common.annotations.VisibleForTesting;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This migration is to prevent multiple permissions for the same user and workspace/organization.
 * For example, a particular user should not be able to have multiple Workspace-level permissions
 * for the same workspace.
 */
public class V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResource extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResource.class);
  private static final Table PERMISSION_TABLE = DSL.table("permission");
  private static final String ORGANIZATION_ID = "organization_id";
  private static final String WORKSPACE_ID = "workspace_id";
  private static final String USER_ID = "user_id";
  private static final String UNIQUE_CONSTRAINT_NAME_WORKSPACE = "permission_unique_user_workspace";
  private static final String UNIQUE_CONSTRAINT_NAME_ORG = "permission_unique_user_organization";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    migrate(ctx);
    LOGGER.info("Migration finished!");
  }

  @VisibleForTesting
  static void migrate(final DSLContext ctx) {
    addUniqueConstraints(ctx);
  }

  private static void addUniqueConstraints(final DSLContext ctx) {
    // Unique constraint for workspace_id and user_id
    ctx.alterTable(PERMISSION_TABLE)
        .add(constraint(UNIQUE_CONSTRAINT_NAME_WORKSPACE)
            .unique(DSL.field(USER_ID), DSL.field(WORKSPACE_ID)))
        .execute();

    // Unique constraint for organization_id and user_id
    ctx.alterTable(PERMISSION_TABLE)
        .add(constraint(UNIQUE_CONSTRAINT_NAME_ORG)
            .unique(DSL.field(USER_ID), DSL.field(ORGANIZATION_ID)))
        .execute();
  }

}
