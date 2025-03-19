/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V1_1_0_005__UpdateConnectorRolloutStateEnumTest extends AbstractConfigsDatabaseTest {

  private static final String CONNECTOR_ROLLOUT_TABLE = "connector_rollout";
  private static final String STATE_COLUMN = "state";
  private static final String CANCELED_ROLLED_BACK = "canceled_rolled_back";
  private static final String CANCELED = "canceled";
  private ConfigsDatabaseMigrator configsDbMigrator;

  @BeforeEach
  void setUp() {
    final Flyway flyway = FlywayFactory.create(dataSource, "V1_1_0_005__UpdateConnectorRolloutStateEnumTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    // Initialize the database with migrations up to, but not including, our target migration
    final BaseJavaMigration previousMigration = new V1_1_0_004__UpdateConfigOriginTypeEnum();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testMigration() {
    final DSLContext ctx = getDslContext();
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_actor_definition_id").execute();
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_initial_version_id").execute();
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_release_candidate_version_id").execute();
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_updated_by").execute();

    // Insert a record with state CANCELED_ROLLED_BACK before migration
    UUID canceledRolledBackId = insertRecordWithStateCanceledRolledBack(ctx);
    assertNotNull(canceledRolledBackId);

    V1_1_0_005__UpdateConnectorRolloutStateEnum.runMigration(ctx);

    verifyAllRecordsUpdated(ctx);
    assertThrows(Exception.class, () -> insertRecordWithStateCanceledRolledBack(ctx));
    UUID connectorRolloutId = insertRecordWithStateCanceled(ctx);
    assertNotNull(connectorRolloutId);
  }

  private UUID insertRecordWithStateCanceledRolledBack(final DSLContext ctx) {
    UUID rolloutId = UUID.randomUUID();
    ctx.insertInto(DSL.table(CONNECTOR_ROLLOUT_TABLE))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("release_candidate_version_id"),
            DSL.field("initial_version_id"),
            DSL.field("state"),
            DSL.field("initial_rollout_pct"),
            DSL.field("current_target_rollout_pct"),
            DSL.field("final_target_rollout_pct"),
            DSL.field("has_breaking_changes"),
            DSL.field("max_step_wait_time_mins"),
            DSL.field("updated_by"),
            DSL.field("created_at"),
            DSL.field("updated_at"),
            DSL.field("completed_at"),
            DSL.field("expires_at"),
            DSL.field("error_msg"),
            DSL.field("failed_reason"),
            DSL.field("rollout_strategy"))
        .values(
            rolloutId,
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            DSL.field("?::connector_rollout_state_type", "canceled_rolled_back"),
            0,
            0,
            0,
            false,
            0,
            UUID.randomUUID(),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            "",
            "",
            DSL.field("?::connector_rollout_strategy_type", "manual"))
        .execute();
    return rolloutId;
  }

  private UUID insertRecordWithStateCanceled(final DSLContext ctx) {
    UUID rolloutId = UUID.randomUUID();
    ctx.insertInto(DSL.table(CONNECTOR_ROLLOUT_TABLE))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("release_candidate_version_id"),
            DSL.field("initial_version_id"),
            DSL.field("state"),
            DSL.field("initial_rollout_pct"),
            DSL.field("current_target_rollout_pct"),
            DSL.field("final_target_rollout_pct"),
            DSL.field("has_breaking_changes"),
            DSL.field("max_step_wait_time_mins"),
            DSL.field("updated_by"),
            DSL.field("created_at"),
            DSL.field("updated_at"),
            DSL.field("completed_at"),
            DSL.field("expires_at"),
            DSL.field("error_msg"),
            DSL.field("failed_reason"),
            DSL.field("rollout_strategy"))
        .values(
            rolloutId,
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            DSL.field("?::connector_rollout_state_type", "canceled"),
            0,
            0,
            0,
            false,
            0,
            UUID.randomUUID(),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            "",
            "",
            DSL.field("?::connector_rollout_strategy_type", "manual"))
        .execute();
    return rolloutId;
  }

  private void verifyAllRecordsUpdated(final DSLContext ctx) {
    int count = ctx.selectCount()
        .from(CONNECTOR_ROLLOUT_TABLE)
        .where(DSL.field(STATE_COLUMN).cast(String.class).eq(CANCELED_ROLLED_BACK))
        .fetchOne(0, int.class);
    assertEquals(0, count, "There should be no CANCELED_ROLLED_BACK records after migration");

    count = ctx.selectCount()
        .from(CONNECTOR_ROLLOUT_TABLE)
        .where(DSL.field(STATE_COLUMN).cast(String.class).eq(CANCELED))
        .fetchOne(0, int.class);
    assertTrue(count > 0, "There should be at least one CANCELED record after migration");
  }

}
