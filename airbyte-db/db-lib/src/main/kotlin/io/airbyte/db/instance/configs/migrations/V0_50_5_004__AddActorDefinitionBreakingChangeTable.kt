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
 * Adds a new table to the configs database to track connector breaking changes.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_5_004__AddActorDefinitionBreakingChangeTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createBreakingChangesTable(ctx)
  }

  companion object {
    @JvmStatic
    @VisibleForTesting
    fun createBreakingChangesTable(ctx: DSLContext) {
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val version = DSL.field("version", SQLDataType.VARCHAR(256).nullable(false))
      val migrationDocumentationUrl =
        DSL.field("migration_documentation_url", SQLDataType.VARCHAR(256).nullable(false))
      val upgradeDeadline = DSL.field("upgrade_deadline", SQLDataType.DATE.nullable(false))
      val message = DSL.field("message", SQLDataType.VARCHAR(256).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("actor_definition_breaking_change")
        .primaryKey(actorDefinitionId, version)
        .constraint(DSL.foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade())
        .columns(
          actorDefinitionId,
          version,
          migrationDocumentationUrl,
          upgradeDeadline,
          message,
          createdAt,
          updatedAt,
        ).execute()
    }
  }
}
