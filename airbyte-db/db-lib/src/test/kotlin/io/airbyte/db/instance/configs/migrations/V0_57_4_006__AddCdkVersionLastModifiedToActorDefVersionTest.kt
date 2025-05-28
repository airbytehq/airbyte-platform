/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersionTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersionTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersion()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun addCdkVersionToActorDefinitionVersion() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    // tests that the column was added
    Assertions.assertTrue(columnExists(ctx, "actor_definition_version", "cdk_version"))

    // tests that we can insert a row with a cdk_version
    insertADVWithCdk(ctx, "python:0.1.0")
    insertADVWithCdk(ctx, null)
  }

  @Test
  fun addLastPublishedToActorDefinitionVersion() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    // tests that the column was added
    Assertions.assertTrue(columnExists(ctx, "actor_definition_version", "last_published"))

    // tests that we can insert a row with a last_published
    insertADVWithLastPublished(ctx, "2021-01-01T00:00:00Z")
    insertADVWithLastPublished(ctx, "2024-05-30T15:02:26.841000+00:00")
    insertADVWithLastPublished(ctx, null)
  }

  @Test
  fun addMetricsToActorDefinition() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    // tests that the column was added
    Assertions.assertTrue(columnExists(ctx, "actor_definition", "metrics"))

    // tests that we can insert a row with metrics
    insertADWithMetrics(ctx, "{}")
    insertADWithMetrics(ctx, "{\"foo\": \"bar\"}")
    insertADWithMetrics(ctx, null)
  }

  companion object {
    private fun columnExists(
      ctx: DSLContext,
      tableName: String,
      columnName: String,
    ): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq(tableName)
              .and(DSL.field("column_name").eq(columnName)),
          ),
      )

    private fun insertADVWithCdk(
      ctx: DSLContext,
      cdkVersion: String?,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
          DSL.field("release_stage"),
          DSL.field("support_level"),
          DSL.field("cdk_version"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "repo",
          "1.0.0",
          JSONB.valueOf("{}"),
          ReleaseStage.alpha,
          SupportLevel.community,
          cdkVersion,
        ).execute()
    }

    private fun insertADVWithLastPublished(
      ctx: DSLContext,
      lastPublished: String?,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
          DSL.field("release_stage"),
          DSL.field("support_level"),
          DSL.field("last_published"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "repo",
          "1.0.0",
          JSONB.valueOf("{}"),
          ReleaseStage.alpha,
          SupportLevel.community,
          if (lastPublished != null) OffsetDateTime.parse(lastPublished) else null,
        ).execute()
    }

    private fun insertADWithMetrics(
      ctx: DSLContext,
      metrics: String?,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("actor_type"),
          DSL.field("metrics"),
        ).values(
          UUID.randomUUID(),
          "name",
          ActorType.source,
          JSONB.valueOf(metrics),
        ).execute()
    }
  }
}
