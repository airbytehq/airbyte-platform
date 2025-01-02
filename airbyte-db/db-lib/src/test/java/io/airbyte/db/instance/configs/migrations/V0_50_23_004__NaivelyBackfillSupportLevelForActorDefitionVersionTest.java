/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage;
import io.airbyte.db.instance.configs.migrations.V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersionTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_23_003__AddSupportLevelToActorDefinitionVersionTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private static void insertAdvWithReleaseStageAndSupportLevel(
                                                               final DSLContext ctx,
                                                               final ReleaseStage releaseStage,
                                                               final SupportLevel supportLevel) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("support_level"),
            DSL.field("release_stage"))
        .values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "repo",
            "1.0.0",
            JSONB.valueOf("{}"),
            supportLevel,
            releaseStage)
        .execute();
  }

  private static void insertWithoutSupportLevel(final DSLContext ctx) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("release_stage"))
        .values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "repo",
            "1.0.0",
            JSONB.valueOf("{}"),
            ReleaseStage.alpha)
        .execute();
  }

  @Test
  void testBackfillSupportLevel() throws Exception {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final int numberOfAdvs = 10;

    insertAdvWithReleaseStageAndSupportLevel(ctx, ReleaseStage.alpha, SupportLevel.certified);

    for (int i = 0; i < numberOfAdvs; i++) {
      insertAdvWithReleaseStageAndSupportLevel(ctx, ReleaseStage.alpha, SupportLevel.none);
      insertAdvWithReleaseStageAndSupportLevel(ctx, ReleaseStage.beta, SupportLevel.none);
      insertAdvWithReleaseStageAndSupportLevel(ctx, ReleaseStage.generally_available, SupportLevel.none);
      insertAdvWithReleaseStageAndSupportLevel(ctx, ReleaseStage.custom, SupportLevel.none);
    }

    // assert that all advs have support level "none"
    final List<Record> preAdvs = ctx.select()
        .from(DSL.table("actor_definition_version"))
        .where(DSL.field("support_level").eq(SupportLevel.none))
        .fetch();

    Assertions.assertEquals(numberOfAdvs * 4, preAdvs.size());

    V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.backfillSupportLevel(ctx);

    // assert that all alpha advs have support level set to community
    final List<Record> alphaAdvs = ctx.select()
        .from(DSL.table("actor_definition_version"))
        .where(DSL.field("release_stage").eq(ReleaseStage.alpha).and(DSL.field("support_level").eq(SupportLevel.community)))
        .fetch();
    Assertions.assertEquals(numberOfAdvs, alphaAdvs.size());

    // assert that all beta advs have support level set to community
    final List<Record> betaAdvs = ctx.select()
        .from(DSL.table("actor_definition_version"))
        .where(DSL.field("release_stage").eq(ReleaseStage.beta).and(DSL.field("support_level").eq(SupportLevel.community)))
        .fetch();

    Assertions.assertEquals(numberOfAdvs, betaAdvs.size());

    // assert that all generally_available advs have support level set to certified
    final List<Record> gaAdvs = ctx.select()
        .from(DSL.table("actor_definition_version"))
        .where(DSL.field("release_stage").eq(ReleaseStage.generally_available).and(DSL.field("support_level").eq(SupportLevel.certified)))
        .fetch();

    Assertions.assertEquals(numberOfAdvs, gaAdvs.size());

    // assert that all custom advs have support level set to none
    final List<Record> customAdvs = ctx.select()
        .from(DSL.table("actor_definition_version"))
        .where(DSL.field("release_stage").eq(ReleaseStage.custom).and(DSL.field("support_level").eq(SupportLevel.none)))
        .fetch();

    Assertions.assertEquals(numberOfAdvs, customAdvs.size());

    // assert that there is one adv with support level certified and release stage alpha (i.e. did not
    // get overwritten)
    final List<Record> certifiedAdvs = ctx.select()
        .from(DSL.table("actor_definition_version"))
        .where(DSL.field("release_stage").eq(ReleaseStage.alpha).and(DSL.field("support_level").eq(SupportLevel.certified)))
        .fetch();

    Assertions.assertEquals(1, certifiedAdvs.size());
  }

  @Test
  void testNoDefaultSupportLevel() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.backfillSupportLevel(ctx);

    Assertions.assertThrows(RuntimeException.class, () -> insertWithoutSupportLevel(ctx));
  }

}
