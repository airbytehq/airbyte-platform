/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage;
import io.airbyte.db.instance.configs.migrations.V0_50_41_006__AlterSupportLevelAddArchived.SupportLevel;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_41_006__AlterSupportLevelAddArchivedTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_41_006__AlterSupportLevelAddArchivedTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_41_006__AlterSupportLevelAddArchived();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private static void insertAdvWithSupportLevel(
                                                final DSLContext ctx,
                                                final SupportLevel supportLevel) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("release_stage"),
            DSL.field("support_level"))
        .values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "repo",
            "1.0.0",
            JSONB.valueOf("{}"),
            ReleaseStage.alpha,
            supportLevel)
        .execute();
  }

  @Test
  void testArchivedConnectorVersion() throws Exception {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    insertAdvWithSupportLevel(ctx, SupportLevel.archived); // does not throw

    assertThrows(IllegalArgumentException.class, () -> insertAdvWithSupportLevel(ctx, SupportLevel.valueOf("foo")));
  }

}
