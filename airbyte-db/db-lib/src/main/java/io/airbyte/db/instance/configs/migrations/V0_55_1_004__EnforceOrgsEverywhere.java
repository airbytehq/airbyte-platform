/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_55_1_004__EnforceOrgsEverywhere extends BaseJavaMigration {

  private static final String WORKSPACE_TABLE = "workspace";
  private static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final Field<UUID> ORGANIZATION_ID_COLUMN = DSL.field("organization_id", SQLDataType.UUID);
  private static final Field<OffsetDateTime> UPDATED_AT_COLUMN = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Logger LOGGER = LoggerFactory.getLogger(V0_55_1_004__EnforceOrgsEverywhere.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    putAllWorkspacesWithoutOrgIntoDefaultOrg(ctx);
    setOrganizationIdNotNull(ctx);
  }

  public static void putAllWorkspacesWithoutOrgIntoDefaultOrg(DSLContext ctx) {
    ctx.update(DSL.table(WORKSPACE_TABLE))
        .set(ORGANIZATION_ID_COLUMN, DEFAULT_ORGANIZATION_ID)
        .set(UPDATED_AT_COLUMN, currentOffsetDateTime())
        .where(ORGANIZATION_ID_COLUMN.isNull())
        .execute();
  }

  public static void setOrganizationIdNotNull(DSLContext ctx) {
    ctx.alterTable(WORKSPACE_TABLE).alterColumn(ORGANIZATION_ID_COLUMN).setNotNull().execute();
  }

}
