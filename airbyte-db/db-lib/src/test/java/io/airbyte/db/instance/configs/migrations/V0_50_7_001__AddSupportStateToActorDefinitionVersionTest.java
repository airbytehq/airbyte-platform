/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_7_001__AddSupportStateToActorDefinitionVersion.SupportState;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.io.IOException;
import java.sql.SQLException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_7_001__AddSupportStateToActorDefinitionVersionTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_7_001__AddSupportStateTest.java", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_6_002__AddDefaultVersionIdToActor();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() throws IOException, SQLException {
    final DSLContext context = getDslContext();
    V0_50_7_001__AddSupportStateToActorDefinitionVersion.addSupportStateType(context);
    V0_50_7_001__AddSupportStateToActorDefinitionVersion.addSupportStateColumnToActorDefinitionVersion(context);
    assertTrue(typeExists(context, "support_state"));
    assertTrue(columnExists(context, "support_state", "actor_definition_version"));

    // All support states should be set to "supported" after migration that added the column
    assertEquals(
        context.fetchCount(
            DSL.select()
                .from("actor_definition_version")),
        context.fetchCount(
            DSL.select()
                .from("actor_definition_version")
                .where(DSL.field("support_state").eq(SupportState.supported))));
  }

  static boolean typeExists(final DSLContext ctx, final String typeName) {
    return ctx.fetchExists(DSL.select()
        .from("pg_type")
        .where(DSL.field("typname").eq(typeName)));
  }

  static boolean columnExists(final DSLContext ctx, final String columnName, final String tableName) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq(tableName)
            .and(DSL.field("column_name").eq(columnName))));
  }

}
