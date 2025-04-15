/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Persist discover schema catalog.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_26_001__PersistDiscoveredCatalog : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  companion object {
    private const val ACTOR_CATALOG = "actor_catalog"

    @JvmStatic
    @VisibleForTesting
    fun migrate(ctx: DSLContext) {
      createActorCatalog(ctx)
      createCatalogFetchEvent(ctx)
      addConnectionTableForeignKey(ctx)
    }

    private fun createActorCatalog(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val catalog = DSL.field("catalog", SQLDataType.JSONB.nullable(false))
      val catalogHash = DSL.field("catalog_hash", SQLDataType.VARCHAR(32).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
      ctx
        .createTableIfNotExists(ACTOR_CATALOG)
        .columns(
          id,
          catalog,
          catalogHash,
          createdAt,
        ).constraints(DSL.primaryKey(id))
        .execute()
      log.info { "actor_catalog table created" }
      ctx.createIndexIfNotExists("actor_catalog_catalog_hash_id_idx").on(ACTOR_CATALOG, "catalog_hash").execute()
    }

    private fun createCatalogFetchEvent(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val actorCatalogId = DSL.field("actor_catalog_id", SQLDataType.UUID.nullable(false))
      val actorId = DSL.field("actor_id", SQLDataType.UUID.nullable(false))
      val configHash = DSL.field("config_hash", SQLDataType.VARCHAR(32).nullable(false))
      val actorVersion = DSL.field("actor_version", SQLDataType.VARCHAR(256).nullable(false))

      ctx
        .createTableIfNotExists("actor_catalog_fetch_event")
        .columns(
          id,
          actorCatalogId,
          actorId,
          configHash,
          actorVersion,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(actorCatalogId).references(ACTOR_CATALOG, "id").onDeleteCascade(),
          DSL.foreignKey(actorId).references("actor", "id").onDeleteCascade(),
        ).execute()
      log.info { "actor_catalog_fetch_event table created" }
      ctx
        .createIndexIfNotExists("actor_catalog_fetch_event_actor_id_idx")
        .on("actor_catalog_fetch_event", "actor_id")
        .execute()
      ctx
        .createIndexIfNotExists("actor_catalog_fetch_event_actor_catalog_id_idx")
        .on("actor_catalog_fetch_event", "actor_catalog_id")
        .execute()
    }

    private fun addConnectionTableForeignKey(ctx: DSLContext) {
      val sourceCatalogId = DSL.field("source_catalog_id", SQLDataType.UUID.nullable(true))
      ctx
        .alterTable("connection")
        .addIfNotExists(sourceCatalogId)
        .execute()
      ctx
        .alterTable("connection")
        .dropConstraintIfExists("connection_actor_catalog_id_fk")
      ctx
        .alterTable("connection")
        .add(
          DSL
            .constraint("connection_actor_catalog_id_fk")
            .foreignKey(sourceCatalogId)
            .references(ACTOR_CATALOG, "id")
            .onDeleteCascade(),
        ).execute()
    }
  }
}
