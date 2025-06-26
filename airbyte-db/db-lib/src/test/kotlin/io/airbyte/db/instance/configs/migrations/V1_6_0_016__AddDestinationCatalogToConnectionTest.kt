/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
class V1_6_0_016__AddDestinationCatalogToConnectionTest : AbstractConfigsDatabaseTest() {
  companion object {
    private const val ACTOR_CATALOG_TABLE = "actor_catalog"
    private const val CATALOG_TYPE_COLUMN = "catalog_type"
    private const val ID_COLUMN = "id"
    private const val CATALOG_COLUMN = "catalog"
    private const val CATALOG_HASH_COLUMN = "catalog_hash"
    private const val CREATED_AT_COLUMN = "created_at"
    private const val MODIFIED_AT_COLUMN = "modified_at"
  }

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_6_0_016__AddDestinationCatalogToConnectionTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_6_0_015__PreparePermissionTableForServiceAccounts()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun `test existing rows are defaulted to SOURCE_CATALOG after migration`() {
    val ctx = dslContext!!
    val now = OffsetDateTime.now()

    // Insert rows before the catalog_type column exists
    val existingCatalogId1 = UUID.randomUUID()

    ctx
      .insertInto(DSL.table(ACTOR_CATALOG_TABLE))
      .columns(
        DSL.field(ID_COLUMN),
        DSL.field(CATALOG_COLUMN),
        DSL.field(CATALOG_HASH_COLUMN),
        DSL.field(CREATED_AT_COLUMN),
        DSL.field(MODIFIED_AT_COLUMN),
      ).values(
        existingCatalogId1,
        JSONB.jsonb("{}"),
        "existinghash1",
        now,
        now,
      ).execute()

    // Run the migration to add catalog_type column
    V1_6_0_016__AddDestinationCatalogToConnection.addActorCatalogTypeColumn(ctx)

    // Verify existing rows are defaulted to SOURCE_CATALOG
    val result1 =
      ctx
        .selectFrom(DSL.table(ACTOR_CATALOG_TABLE))
        .where(DSL.field(ID_COLUMN).eq(existingCatalogId1))
        .fetchOne()

    assertEquals(V1_6_0_016__AddDestinationCatalogToConnection.ActorCatalogType.SOURCE_CATALOG, result1?.get(CATALOG_TYPE_COLUMN))
  }

  @Test
  fun `test can insert actor catalog with different catalog types`() {
    val ctx = dslContext!!

    // Run the migration to add catalog_type column
    V1_6_0_016__AddDestinationCatalogToConnection.addActorCatalogTypeColumn(ctx)

    // Insert a source catalog
    val sourceCatalogId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    ctx
      .insertInto(DSL.table(ACTOR_CATALOG_TABLE))
      .columns(
        DSL.field(ID_COLUMN),
        DSL.field(CATALOG_COLUMN),
        DSL.field(CATALOG_HASH_COLUMN),
        DSL.field(CREATED_AT_COLUMN),
        DSL.field(MODIFIED_AT_COLUMN),
        DSL.field(CATALOG_TYPE_COLUMN),
      ).values(
        sourceCatalogId,
        JSONB.jsonb("{}"),
        "dummyhashsource",
        now,
        now,
        V1_6_0_016__AddDestinationCatalogToConnection.ActorCatalogType.SOURCE_CATALOG,
      ).execute()

    // Insert a destination catalog
    val destCatalogId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table(ACTOR_CATALOG_TABLE))
      .columns(
        DSL.field(ID_COLUMN),
        DSL.field(CATALOG_COLUMN),
        DSL.field(CATALOG_HASH_COLUMN),
        DSL.field(CREATED_AT_COLUMN),
        DSL.field(MODIFIED_AT_COLUMN),
        DSL.field(CATALOG_TYPE_COLUMN),
      ).values(
        destCatalogId,
        JSONB.jsonb("{}"),
        "dummyhashdest",
        now,
        now,
        V1_6_0_016__AddDestinationCatalogToConnection.ActorCatalogType.DESTINATION_CATALOG,
      ).execute()

    // Verify both were inserted correctly
    val sourceResult =
      ctx
        .selectFrom(DSL.table(ACTOR_CATALOG_TABLE))
        .where(DSL.field(ID_COLUMN).eq(sourceCatalogId))
        .fetchOne()

    val destResult =
      ctx
        .selectFrom(DSL.table(ACTOR_CATALOG_TABLE))
        .where(DSL.field(ID_COLUMN).eq(destCatalogId))
        .fetchOne()

    assertEquals(V1_6_0_016__AddDestinationCatalogToConnection.ActorCatalogType.SOURCE_CATALOG, sourceResult?.get(CATALOG_TYPE_COLUMN))
    assertEquals(V1_6_0_016__AddDestinationCatalogToConnection.ActorCatalogType.DESTINATION_CATALOG, destResult?.get(CATALOG_TYPE_COLUMN))
  }
}
