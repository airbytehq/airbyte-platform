/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersionTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersionTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersion();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private static boolean columnExists(final DSLContext ctx, final String tableName, final String columnName) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq(tableName)
            .and(DSL.field("column_name").eq(columnName))));
  }

  private static void insertADVWithCdk(
                                       final DSLContext ctx,
                                       final String cdkVersion) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("release_stage"),
            DSL.field("support_level"),
            DSL.field("cdk_version"))
        .values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "repo",
            "1.0.0",
            JSONB.valueOf("{}"),
            ReleaseStage.alpha,
            SupportLevel.community,
            cdkVersion)
        .execute();
  }

  private static void insertADVWithLastPublished(
                                                 final DSLContext ctx,
                                                 final String lastPublished) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("spec"),
            DSL.field("release_stage"),
            DSL.field("support_level"),
            DSL.field("last_published"))
        .values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "repo",
            "1.0.0",
            JSONB.valueOf("{}"),
            ReleaseStage.alpha,
            SupportLevel.community,
            lastPublished != null ? OffsetDateTime.parse(lastPublished) : null)
        .execute();
  }

  private static void insertADWithMetrics(
                                          final DSLContext ctx,
                                          final String metrics) {
    ctx.insertInto(DSL.table("actor_definition"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"),
            DSL.field("metrics"))
        .values(
            UUID.randomUUID(),
            "name",
            ActorType.source,
            JSONB.valueOf(metrics))
        .execute();
  }

  @Test
  void addCdkVersionToActorDefinitionVersion() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    // tests that the column was added
    Assertions.assertTrue(columnExists(ctx, "actor_definition_version", "cdk_version"));

    // tests that we can insert a row with a cdk_version
    insertADVWithCdk(ctx, "python:0.1.0");
    insertADVWithCdk(ctx, null);
  }

  @Test
  void addLastPublishedToActorDefinitionVersion() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    // tests that the column was added
    Assertions.assertTrue(columnExists(ctx, "actor_definition_version", "last_published"));

    // tests that we can insert a row with a last_published
    insertADVWithLastPublished(ctx, "2021-01-01T00:00:00Z");
    insertADVWithLastPublished(ctx, "2024-05-30T15:02:26.841000+00:00");
    insertADVWithLastPublished(ctx, null);
  }

  @Test
  void addMetricsToActorDefinition() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    // tests that the column was added
    Assertions.assertTrue(columnExists(ctx, "actor_definition", "metrics"));

    // tests that we can insert a row with metrics
    insertADWithMetrics(ctx, "{}");
    insertADWithMetrics(ctx, "{\"foo\": \"bar\"}");
    insertADWithMetrics(ctx, null);
  }

}
