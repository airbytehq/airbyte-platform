/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.*;

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

class V1_1_0_004__UpdateConfigOriginTypeEnumTest extends AbstractConfigsDatabaseTest {

  private static final String SCOPED_CONFIGURATION_TABLE = "scoped_configuration";
  private static final String ORIGIN_TYPE_COLUMN = "origin_type";
  private static final String RELEASE_CANDIDATE = "release_candidate";
  private static final String CONNECTOR_ROLLOUT = "connector_rollout";

  private ConfigsDatabaseMigrator configsDbMigrator;

  @BeforeEach
  void setUp() {
    final Flyway flyway = FlywayFactory.create(dataSource, "V1_1_0_004__UpdateConfigOriginTypeEnumTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    // Initialize the database with migrations up to, but not including, our target migration
    final BaseJavaMigration previousMigration = new V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testMigration() {
    final DSLContext ctx = getDslContext();
    // Insert a record with RELEASE_CANDIDATE before migration
    UUID releaseCandidateId = insertRecordWithOriginReleaseCandidate(ctx);
    assertNotNull(releaseCandidateId);

    V1_1_0_004__UpdateConfigOriginTypeEnum.runMigration(ctx);

    verifyAllRecordsUpdated(ctx);
    assertThrows(Exception.class, () -> insertRecordWithOriginReleaseCandidate(ctx));
    UUID connectorRolloutId = insertRecordWithOriginConnectorRollout(ctx);
    assertNotNull(connectorRolloutId);
  }

  private UUID insertRecordWithOriginReleaseCandidate(final DSLContext ctx) {
    UUID configId = UUID.randomUUID();
    ctx.insertInto(DSL.table(SCOPED_CONFIGURATION_TABLE))
        .columns(
            DSL.field("id"),
            DSL.field("key"),
            DSL.field("resource_type"),
            DSL.field("resource_id"),
            DSL.field("scope_type"),
            DSL.field("scope_id"),
            DSL.field("value"),
            DSL.field("description"),
            DSL.field("reference_url"),
            DSL.field("origin_type"),
            DSL.field("origin"),
            DSL.field("expires_at"))
        .values(
            configId,
            "testKey",
            DSL.field("?::config_resource_type", "actor_definition"),
            UUID.randomUUID(),
            DSL.field("?::config_scope_type", "workspace"),
            UUID.randomUUID(),
            "testValue",
            "testDescription",
            "testUrl",
            DSL.field("?::config_origin_type", "release_candidate"),
            "testOrigin",
            OffsetDateTime.now())
        .execute();
    return configId;
  }

  private UUID insertRecordWithOriginConnectorRollout(final DSLContext ctx) {
    UUID configId = UUID.randomUUID();
    ctx.insertInto(DSL.table(SCOPED_CONFIGURATION_TABLE))
        .columns(
            DSL.field("id"),
            DSL.field("key"),
            DSL.field("resource_type"),
            DSL.field("resource_id"),
            DSL.field("scope_type"),
            DSL.field("scope_id"),
            DSL.field("value"),
            DSL.field("description"),
            DSL.field("reference_url"),
            DSL.field("origin_type"),
            DSL.field("origin"),
            DSL.field("expires_at"))
        .values(
            configId,
            "testKey",
            DSL.field("?::config_resource_type", "actor_definition"),
            UUID.randomUUID(),
            DSL.field("?::config_scope_type", "workspace"),
            UUID.randomUUID(),
            "testValue",
            "testDescription",
            "testUrl",
            DSL.field("?::config_origin_type", "connector_rollout"),
            "testOrigin",
            OffsetDateTime.now())
        .execute();
    return configId;
  }

  private void verifyAllRecordsUpdated(final DSLContext ctx) {
    int count = ctx.selectCount()
        .from(SCOPED_CONFIGURATION_TABLE)
        .where(DSL.field(ORIGIN_TYPE_COLUMN).cast(String.class).eq(RELEASE_CANDIDATE))
        .fetchOne(0, int.class);
    assertEquals(0, count, "There should be no RELEASE_CANDIDATE records after migration");

    count = ctx.selectCount()
        .from(SCOPED_CONFIGURATION_TABLE)
        .where(DSL.field(ORIGIN_TYPE_COLUMN).cast(String.class).eq(CONNECTOR_ROLLOUT))
        .fetchOne(0, int.class);
    assertTrue(count > 0, "There should be at least one CONNECTOR_ROLLOUT record after migration");
  }

}
