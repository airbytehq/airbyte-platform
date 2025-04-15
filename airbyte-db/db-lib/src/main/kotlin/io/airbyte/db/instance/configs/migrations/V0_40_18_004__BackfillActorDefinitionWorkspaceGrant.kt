/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Backfill actor definition workspace grant migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_18_004__BackfillActorDefinitionWorkspaceGrant : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    val customActorDefinitionIds = ctx.fetch("SELECT id FROM actor_definition WHERE public is false and tombstone is false;")
    val existingWorkspaces = ctx.fetch("SELECT id FROM WORKSPACE where tombstone is false;")

    // Update for all custom connectors - set custom field to true;
    ctx.execute("UPDATE actor_definition SET custom = true  WHERE public is false and tombstone is false;")

    for (customActorDefinitionIdRecord in customActorDefinitionIds) {
      for (existingWorkspaceRecord in existingWorkspaces) {
        // Populate a record for new table;
        val customActorDefinitionIdValue = customActorDefinitionIdRecord.getValue("id", UUID::class.java)
        val existingWorkspaceIdValue = existingWorkspaceRecord.getValue("id", UUID::class.java)

        ctx.execute(
          "INSERT INTO actor_definition_workspace_grant(workspace_id, actor_definition_id) VALUES ({0}, {1})",
          existingWorkspaceIdValue,
          customActorDefinitionIdValue,
        )
      }
    }
  }
}
