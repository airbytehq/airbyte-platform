/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_017__AddOrganizationIdToActorOauthParameter extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_017__AddOrganizationIdToActorOauthParameter.class);

  private static final Field<UUID> ORGANIZATION_ID_COLUMN = DSL.field("organization_id", SQLDataType.UUID.nullable(true));
  private static final Field<UUID> WORKSPACE_ID_COLUMN = DSL.field("workspace_id", SQLDataType.UUID.nullable(true));
  public static final String ACTOR_OAUTH_PARAMETER = "actor_oauth_parameter";
  public static final String ONLY_WORKSPACE_OR_ORG_IS_SET = "only_workspace_or_org_is_set";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    doMigration(ctx);

  }

  @VisibleForTesting
  static void doMigration(final DSLContext ctx) {
    ctx.alterTable(ACTOR_OAUTH_PARAMETER)
        .addColumnIfNotExists(ORGANIZATION_ID_COLUMN, SQLDataType.UUID.nullable(true)).execute();

    ctx.alterTable(ACTOR_OAUTH_PARAMETER)
        .add(DSL.constraint(ONLY_WORKSPACE_OR_ORG_IS_SET).check(ORGANIZATION_ID_COLUMN.isNull().or(WORKSPACE_ID_COLUMN.isNull()))).execute();
  }

}
