/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable.class);
  private static final String ORGANIZATION_TABLE = "organization";
  private static final String PBA_COLUMN = "pba";
  private static final String ORG_LEVEL_BILLING_COLUMN = "org_level_billing";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    // Add pba column to organization table. This boolean flag will let us know if an organization is a
    // PbA (Powered by Airbyte) customer or not.
    final Field<Boolean> pba = DSL.field(PBA_COLUMN, SQLDataType.BOOLEAN.defaultValue(false).nullable(false));
    ctx.alterTable(ORGANIZATION_TABLE)
        .addColumnIfNotExists(pba)
        .execute();

    // Add org_level_billing column to organization table. This boolean flag will let us know if
    // workspaces in this organization should be billed
    // at the organization level or at the workspace level.
    final Field<Boolean> orgLevelBilling = DSL.field(ORG_LEVEL_BILLING_COLUMN, SQLDataType.BOOLEAN.defaultValue(false).nullable(false));
    ctx.alterTable(ORGANIZATION_TABLE)
        .addColumnIfNotExists(orgLevelBilling)
        .execute();

    LOGGER.info("Migration finished!");
  }

}
