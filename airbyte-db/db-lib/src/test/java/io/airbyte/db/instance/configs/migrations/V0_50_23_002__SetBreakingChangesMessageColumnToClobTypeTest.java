/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.configs.migrations.V0_50_23_002__SetBreakingChangesMessageColumnToClobType.alterMessageColumnType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_23_002__SetBreakingChangesMessageColumnToClobTypeTest extends AbstractConfigsDatabaseTest {

  private static final Table<Record> ACTOR_DEFINITION_BREAKING_CHANGE = DSL.table("actor_definition_breaking_change");
  private static final Table<Record> ACTOR_DEFINITION = DSL.table("actor_definition");
  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_23_002__SetBreakingChangesMessageColumnToClobTypeTest.java", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testInsertThrowsBeforeMigration() {
    final DSLContext ctx = getDslContext();
    insertActorDefinitionDependency(ctx);
    final Throwable exception = assertThrows(DataAccessException.class, () -> insertBreakingChange(ctx));
    assertTrue(exception.getMessage().contains("value too long for type character varying(256)"));
  }

  @Test
  void testInsertSucceedsAfterMigration() {
    final DSLContext ctx = getDslContext();
    insertActorDefinitionDependency(ctx);
    alterMessageColumnType(ctx);
    assertDoesNotThrow(() -> insertBreakingChange(ctx));
  }

  private static void insertActorDefinitionDependency(final DSLContext ctx) {
    ctx.insertInto(ACTOR_DEFINITION)
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"))
        .values(
            ACTOR_DEFINITION_ID,
            "source def name",
            ActorType.source)
        .onConflict(
            DSL.field("id"))
        .doNothing()
        .execute();
  }

  private static void insertBreakingChange(final DSLContext ctx) {
    final String message =
        "This version introduces [Destinations V2](https://docs.airbyte.com/release_notes/upgrading_to_destinations_v2/#what-is-destinations-v2), which provides better error handling, incremental delivery of data for large syncs, and improved final table structures. To review the breaking changes, and how to upgrade, see [here](https://docs.airbyte.com/release_notes/upgrading_to_destinations_v2/#quick-start-to-upgrading). These changes will likely require updates to downstream dbt / SQL models, which we walk through [here](https://docs.airbyte.com/release_notes/upgrading_to_destinations_v2/#updating-downstream-transformations). Selecting `Upgrade` will upgrade **all** connections using this destination at their next sync. You can manually sync existing connections prior to the next scheduled sync to start the upgrade early.";

    ctx.insertInto(ACTOR_DEFINITION_BREAKING_CHANGE)
        .columns(
            DSL.field("actor_definition_id"),
            DSL.field("version"),
            DSL.field("upgrade_deadline"),
            DSL.field("message"),
            DSL.field("migration_documentation_url"),
            DSL.field("created_at"),
            DSL.field("updated_at"))
        .values(
            ACTOR_DEFINITION_ID,
            "3.0.0",
            Date.valueOf("2023-11-01"),
            message,
            "https://docs.airbyte.com/integrations/destinations/snowflake-migrations#3.0.0",
            Timestamp.valueOf("2023-08-25 16:33:42.701943875"),
            Timestamp.valueOf("2023-08-25 16:33:42.701943875"))
        .onConflict(
            DSL.field("actor_definition_id"),
            DSL.field("version"))
        .doUpdate()
        .set(DSL.field("upgrade_deadline"), Date.valueOf("2023-11-01"))
        .set(DSL.field("message"), message)
        .set(DSL.field("migration_documentation_url"), "https://docs.airbyte.com/integrations/destinations/snowflake-migrations#3.0.0")
        .set(DSL.field("updated_at"), Timestamp.valueOf("2023-08-25 16:33:42.701943875"))
        .execute();

  }

}
