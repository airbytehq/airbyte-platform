/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.AIRBYTE_RELEASE_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Creates the catalog of published Airbyte application releases.
 *
 * One row per published version. The table is global release metadata, not customer data, so it has
 * no organization_id. published_at is when chart publication completed; created_at is when Airbyte
 * recorded the row.
 *
 * The schema is reversible with a follow-up migration that drops the table. Flyway migrations are
 * not rolled back in place.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_051__CreateAirbyteReleaseTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute(
      """
      CREATE TABLE $AIRBYTE_RELEASE_TABLE (
        id UUID PRIMARY KEY,
        version VARCHAR(256) NOT NULL,
        source_commit VARCHAR(256) NOT NULL,
        published_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT airbyte_release_version_key UNIQUE (version)
      )
      """.trimIndent(),
    )
  }
}
