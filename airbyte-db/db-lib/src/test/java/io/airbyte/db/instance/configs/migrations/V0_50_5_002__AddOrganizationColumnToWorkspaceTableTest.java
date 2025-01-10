/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.io.IOException;
import java.sql.SQLException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_5_002__AddOrganizationColumnToWorkspaceTableTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_5_002__AddOrganizationColumnToWorkspaceTableTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_5_001__CreateOrganizationTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() throws IOException, SQLException {
    final DSLContext context = getDslContext();
    assertFalse(foreignKeyExists(context));
    V0_50_5_002__AddOrganizationColumnToWorkspaceTable.addOrganizationColumnToWorkspace(context);
    assertTrue(foreignKeyExists(context));
    assertTrue(columnExists(context, "organization_id"));
  }

  static boolean columnExists(final DSLContext ctx, final String columnName) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq("workspace")
            .and(DSL.field("column_name").eq(columnName))));
  }

  protected static boolean foreignKeyExists(final DSLContext ctx) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.table_constraints")
        .where(DSL.field("table_name").eq("workspace")
            .and(DSL.field("constraint_name").eq("workspace_organization_id_fkey"))));
  }

}
