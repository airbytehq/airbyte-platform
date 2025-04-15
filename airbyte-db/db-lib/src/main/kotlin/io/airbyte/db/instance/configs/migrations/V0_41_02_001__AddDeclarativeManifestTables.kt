/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add DeclarativeManifest and ActiveDeclarativeManifest.
 */
@Suppress("ktlint:standard:class-naming")
class V0_41_02_001__AddDeclarativeManifestTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addDeclarativeManifestTable(ctx)
    addActiveDeclarativeManifestTable(ctx)
  }

  companion object {
    private fun addDeclarativeManifestTable(ctx: DSLContext) {
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val description = DSL.field("description", SQLDataType.VARCHAR(256).nullable(false))
      val manifest = DSL.field("manifest", SQLDataType.JSONB.nullable(false))
      val spec = DSL.field("spec", SQLDataType.JSONB.nullable(false))
      val version = DSL.field("version", SQLDataType.BIGINT.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("declarative_manifest")
        .columns(actorDefinitionId, description, manifest, spec, version, createdAt)
        .constraints(DSL.primaryKey(actorDefinitionId, version))
        .execute()
    }

    private fun addActiveDeclarativeManifestTable(ctx: DSLContext) {
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val version = DSL.field("version", SQLDataType.BIGINT.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("active_declarative_manifest")
        .columns(actorDefinitionId, version, createdAt, updatedAt)
        .constraints(
          DSL.primaryKey(actorDefinitionId),
          DSL.foreignKey(actorDefinitionId, version).references("declarative_manifest", "actor_definition_id", "version"),
        ).execute()
    }
  }
}
