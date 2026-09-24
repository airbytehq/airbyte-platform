/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

/** Adds persistent draft state to actors. Application rollback leaves the additive column unused. */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_041__AddActorDraftState : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    context.connection.createStatement().use { statement ->
      statement.execute("ALTER TABLE actor ADD COLUMN is_draft boolean NOT NULL DEFAULT false")
    }
  }
}
