/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

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

class V0_50_6_002__AddDefaultVersionIdToActorTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_6_002__AddDefaultVersionIdToActorTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_6_001__DropUnsupportedProtocolFlagCol();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() throws IOException, SQLException {
    final DSLContext context = getDslContext();
    V0_50_6_002__AddDefaultVersionIdToActor.addDefaultVersionIdColumnToActor(context);
    assertTrue(columnExists(context, "default_version_id", "actor"));
    assertTrue(foreignKeyExists(context, "default_version_id", "actor"));
  }

  static boolean columnExists(final DSLContext ctx, final String columnName, final String tableName) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq(tableName)
            .and(DSL.field("column_name").eq(columnName))));
  }

  protected static boolean foreignKeyExists(final DSLContext ctx, final String columnName, final String tableName) {
    final String constraintName = tableName + "_" + columnName + "_fkey";
    return ctx.fetchExists(DSL.select()
        .from("information_schema.table_constraints")
        .where(DSL.field("table_name").eq(tableName)
            .and(DSL.field("constraint_name").eq(constraintName))));
  }

}
