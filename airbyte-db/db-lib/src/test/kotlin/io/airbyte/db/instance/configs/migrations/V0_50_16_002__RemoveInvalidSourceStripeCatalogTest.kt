/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_16_002__RemoveInvalidSourceStripeCatalog.Companion.removeInvalidSourceStripeCatalog
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_16_002__RemoveInvalidSourceStripeCatalogTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_16_002__RemoveInvalidSourceStripeCatalog()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  private fun insertDummyActorCatalog(
    ctx: DSLContext,
    catalogHash: String,
  ): UUID {
    val actorCatalogId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table("actor_catalog"))
      .columns(
        DSL.field("id"),
        DSL.field("catalog"),
        DSL.field("catalog_hash"),
        DSL.field("created_at"),
      ).values(
        actorCatalogId,
        JSONB.valueOf("{}"),
        catalogHash,
        DSL.currentTimestamp(),
      ).execute()
    return actorCatalogId
  }

  private fun insertDummyConnection(
    ctx: DSLContext,
    sourceCatalogId: UUID,
  ): UUID {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    ctx
      .insertInto(DSL.table("connection"))
      .columns(
        DSL.field("id"),
        DSL.field("source_catalog_id"),
        DSL.field("source_id"),
        DSL.field("destination_id"),
        DSL.field("namespace_definition"),
        DSL.field("name"),
        DSL.field("catalog"),
        DSL.field("manual"),
      ).values(
        connectionId,
        sourceCatalogId,
        sourceId,
        destinationId,
        DSL.field("cast(? as namespace_definition_type)", "customformat"),
        "dummyname",
        JSONB.valueOf("{}"),
        false,
      ).execute()
    return connectionId
  }

  @Test
  @Throws(Exception::class)
  fun testRemoveInvalidSourceStripeCatalog() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val badActorCatalogId =
      insertDummyActorCatalog(ctx, V0_50_16_002__RemoveInvalidSourceStripeCatalog.INVALID_CATALOG_CONTENT_HASH)
    val goodActorCatalogId =
      insertDummyActorCatalog(ctx, V0_50_16_002__RemoveInvalidSourceStripeCatalog.VALID_CATALOG_CONTENT_HASH)
    val otherActorCatalogId = insertDummyActorCatalog(ctx, "other_hash")

    val numberOfConnections = 10

    val badConnectionIds = (0..<numberOfConnections).map { _ -> insertDummyConnection(ctx, badActorCatalogId) }.toList()
    val goodConnectionIds = (0..<numberOfConnections).map { _ -> insertDummyConnection(ctx, goodActorCatalogId) }.toList()
    val otherConnectionIds = (0..<numberOfConnections).map { _ -> insertDummyConnection(ctx, otherActorCatalogId) }.toList()
    removeInvalidSourceStripeCatalog(ctx)

    // check that the bad catalog is deleted
    val badCatalogs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_catalog"))
        .where(DSL.field("id").`in`(badActorCatalogId))
        .fetch()

    Assertions.assertEquals(0, badCatalogs.size)

    // check that the good catalog and other catalog is not deleted
    val goodCatalogs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_catalog"))
        .where(DSL.field("id").`in`(goodActorCatalogId).or(DSL.field("id").`in`(otherActorCatalogId)))
        .fetch()

    Assertions.assertEquals(2, goodCatalogs.size)

    // check that the previously bad connections and the good connections reference the good catalog
    // i.e. the bad connections now have a source_catalog_id that references the good catalog
    val previouslyBadConnections: List<Record> =
      ctx
        .select()
        .from(DSL.table("connection"))
        .where(DSL.field("id").`in`(badConnectionIds))
        .fetch()

    Assertions.assertEquals(numberOfConnections, previouslyBadConnections.size)
    Assertions.assertTrue(
      previouslyBadConnections
        .stream()
        .allMatch { r: Record -> r.get(DSL.field("source_catalog_id")) == goodActorCatalogId },
    )

    // check that the good connections still reference the good catalog
    val goodConnections: List<Record> =
      ctx
        .select()
        .from(DSL.table("connection"))
        .where(DSL.field("id").`in`(goodConnectionIds))
        .fetch()

    Assertions.assertEquals(numberOfConnections, goodConnections.size)
    Assertions.assertTrue(
      goodConnections
        .stream()
        .allMatch { r: Record -> r.get(DSL.field("source_catalog_id")) == goodActorCatalogId },
    )

    // check that the other connections still reference the other catalog
    val otherConnections: List<Record> =
      ctx
        .select()
        .from(DSL.table("connection"))
        .where(DSL.field("id").`in`(otherConnectionIds))
        .fetch()

    Assertions.assertEquals(numberOfConnections, otherConnections.size)
    Assertions.assertTrue(
      otherConnections
        .stream()
        .allMatch { r: Record -> r.get(DSL.field("source_catalog_id")) == otherActorCatalogId },
    )
  }
}
