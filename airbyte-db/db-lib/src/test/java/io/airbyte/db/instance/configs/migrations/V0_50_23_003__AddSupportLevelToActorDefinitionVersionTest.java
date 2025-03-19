/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_23_003__AddSupportLevelToActorDefinitionVersionTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_23_003__AddSupportLevelToActorDefinitionVersionTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_6_002__AddDefaultVersionIdToActor();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() throws SQLException, IOException {
    final DSLContext context = getDslContext();

    // ignore all foreign key constraints
    context.execute("SET session_replication_role = replica;");

    Assertions.assertFalse(supportLevelColumnExists(context));

    V0_50_23_003__AddSupportLevelToActorDefinitionVersion.addSupportLevelToActorDefinitionVersion(context);

    Assertions.assertTrue(supportLevelColumnExists(context));

    assertSupportLevelEnumWorks(context);
  }

  private static boolean supportLevelColumnExists(final DSLContext ctx) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq("actor_definition_version")
            .and(DSL.field("column_name").eq("support_level"))));
  }

  private static void assertSupportLevelEnumWorks(final DSLContext ctx) {
    Assertions.assertDoesNotThrow(() -> {
      insertWithSupportLevel(ctx, SupportLevel.community);
      insertWithSupportLevel(ctx, SupportLevel.certified);
      insertWithSupportLevel(ctx, SupportLevel.none);
    });

    Assertions.assertThrows(Exception.class, () -> {
      insertWithSupportLevel(ctx, SupportLevel.valueOf("invalid"));
    });

    Assertions.assertThrows(Exception.class, () -> {
      insertWithSupportLevel(ctx, SupportLevel.valueOf(null));
    });
  }

  private static void insertWithSupportLevel(final DSLContext ctx, final SupportLevel supportLevel) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("support_level"))
        .values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "repo",
            "1.0.0",
            JSONB.valueOf("{}"),
            supportLevel)
        .execute();
  }

}
