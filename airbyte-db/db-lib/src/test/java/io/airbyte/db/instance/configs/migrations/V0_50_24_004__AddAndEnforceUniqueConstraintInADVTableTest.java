/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage;
import io.airbyte.db.instance.configs.migrations.V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel;
import io.airbyte.db.instance.configs.migrations.V0_50_7_001__AddSupportStateToActorDefinitionVersion.SupportState;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
class V0_50_24_004__AddAndEnforceUniqueConstraintInADVTableTest extends AbstractConfigsDatabaseTest {

  final Table<Record> ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version");
  final Field<UUID> ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", UUID.class);
  final Field<String> DOCKER_IMAGE_TAG = DSL.field("docker_image_tag", String.class);

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_24_004__AddAndEnforceUniqueConstraintInADVTableTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private static final JSONB spec = JSONB.valueOf("{\"some\": \"value\"}");

  private static void insertAdv(final DSLContext ctx, final UUID actorDefinitionId, final String dockerImageTag) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("documentation_url"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("protocol_version"),
            DSL.field("allowed_hosts"),
            DSL.field("release_stage"),
            DSL.field("support_state"),
            DSL.field("support_level"))
        .values(
            UUID.randomUUID(),
            actorDefinitionId,
            "https://docs.airbyte.com/integrations/sources/salesforce",
            "airbyte/source-salesforce",
            dockerImageTag,
            spec,
            "0.2.0",
            JSONB.valueOf("{\"hosts\": [\"*.salesforce.com\"]}"),
            ReleaseStage.generally_available,
            SupportState.supported,
            SupportLevel.certified)
        .execute();
  }

  @Test
  void testMigrate() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final UUID actorDefinitionId1 = UUID.randomUUID();
    final UUID actorDefinitionId2 = UUID.randomUUID();
    final UUID actorDefinitionId3 = UUID.randomUUID();

    // Set up a state with multiple versions and some duplicates
    insertAdv(ctx, actorDefinitionId1, "1.0.0");
    insertAdv(ctx, actorDefinitionId1, "1.0.0");
    insertAdv(ctx, actorDefinitionId1, "1.0.0");

    insertAdv(ctx, actorDefinitionId2, "2.0.0");
    insertAdv(ctx, actorDefinitionId2, "2.0.1");

    insertAdv(ctx, actorDefinitionId3, "3.0.0");
    insertAdv(ctx, actorDefinitionId3, "3.0.0");
    insertAdv(ctx, actorDefinitionId3, "3.0.0");
    insertAdv(ctx, actorDefinitionId3, "3.0.1");
    insertAdv(ctx, actorDefinitionId3, "3.0.2");

    // Initial assertions
    assertAdvCount(ctx, actorDefinitionId1, 3);
    assertAdvCount(ctx, actorDefinitionId2, 2);
    assertAdvCount(ctx, actorDefinitionId3, 5);

    assertAdvTagCount(ctx, actorDefinitionId1, "1.0.0", 3);
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.0", 1);
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.1", 1);
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.0", 3);
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.1", 1);
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.2", 1);

    // Run migration
    V0_50_24_004__AddAndEnforceUniqueConstraintInADVTable.migrate(ctx);

    // Assert duplicate rows were dropped
    assertAdvCount(ctx, actorDefinitionId1, 1);
    assertAdvCount(ctx, actorDefinitionId2, 2);
    assertAdvCount(ctx, actorDefinitionId3, 3);

    assertAdvTagCount(ctx, actorDefinitionId1, "1.0.0", 1);
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.0", 1);
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.1", 1);
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.0", 1);
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.1", 1);

    // Attempting to re-insert an existing row should now fail
    assertThrows(DataAccessException.class, () -> insertAdv(ctx, actorDefinitionId1, "1.0.0"));
  }

  private void assertAdvCount(final DSLContext ctx, final UUID actorDefinitionId, final int expectedCount) {
    final int actualCount = ctx.select()
        .from(ACTOR_DEFINITION_VERSION)
        .where(ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

  private void assertAdvTagCount(final DSLContext ctx, final UUID actorDefinitionId, final String dockerImageTag, final int expectedCount) {
    final int actualCount = ctx.select()
        .from(ACTOR_DEFINITION_VERSION)
        .where(ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .and(DOCKER_IMAGE_TAG.eq(dockerImageTag))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

}
