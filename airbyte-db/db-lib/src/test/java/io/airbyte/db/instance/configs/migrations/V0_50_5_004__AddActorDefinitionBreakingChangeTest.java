/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_5_004__AddActorDefinitionBreakingChangeTest extends AbstractConfigsDatabaseTest {

  private static final String ACTOR_DEFINITION_BREAKING_CHANGE = "actor_definition_breaking_change";
  private static final String ACTOR_DEFINITION_ID = "actor_definition_id";
  private static final String VERSION = "version";
  private static final String MIGRATION_DOCUMENTATION_URL = "migration_documentation_url";
  private static final String UPGRADE_DEADLINE = "upgrade_deadline";
  private static final String MESSAGE = "message";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_5_004__AddActorDefinitionBreakingChangeTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_4_002__DropActorDefinitionVersionedCols();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() throws SQLException, IOException {
    final DSLContext context = getDslContext();
    V0_50_5_004__AddActorDefinitionBreakingChangeTable.createBreakingChangesTable(context);

    final UUID actorDefinitionId = UUID.randomUUID();

    context.insertInto(DSL.table("actor_definition"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"))
        .values(
            actorDefinitionId,
            "name",
            ActorType.source)
        .execute();

    // assert can insert
    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(ACTOR_DEFINITION_BREAKING_CHANGE))
          .columns(
              DSL.field(ACTOR_DEFINITION_ID),
              DSL.field(VERSION),
              DSL.field(MIGRATION_DOCUMENTATION_URL),
              DSL.field(UPGRADE_DEADLINE),
              DSL.field(MESSAGE))
          .values(
              actorDefinitionId,
              "1.0.0",
              "https://docs.airbyte.com/migration",
              LocalDate.of(2025, 1, 1),
              "some reason")
          .execute();
    });

    // assert uniqueness for actor def + version
    final Exception e = Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(DSL.table(ACTOR_DEFINITION_BREAKING_CHANGE))
          .columns(
              DSL.field(ACTOR_DEFINITION_ID),
              DSL.field(VERSION),
              DSL.field(MIGRATION_DOCUMENTATION_URL),
              DSL.field(UPGRADE_DEADLINE),
              DSL.field(MESSAGE))
          .values(
              actorDefinitionId,
              "1.0.0",
              "https://docs.airbyte.com/migration/2",
              LocalDate.of(2024, 1, 1),
              "some other reason")
          .execute();
    });
    Assertions.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
  }

}
