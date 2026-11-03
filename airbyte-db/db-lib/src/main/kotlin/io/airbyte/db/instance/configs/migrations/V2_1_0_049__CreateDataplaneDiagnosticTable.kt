/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.DATAPLANE_DIAGNOSTIC_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Creates the table that keeps the latest result of each data plane health diagnostic.
 *
 * One row per data plane, check and tested target (a secret store, or the data plane itself). A new
 * result replaces the row. checked_at is when the launcher ran the check; updated_at is when Airbyte
 * last received it.
 *
 * The schema is reversible with a follow-up migration that drops the table. Flyway migrations are
 * not rolled back in place.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_049__CreateDataplaneDiagnosticTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute(
      """
      CREATE TABLE $DATAPLANE_DIAGNOSTIC_TABLE (
        id UUID PRIMARY KEY,
        dataplane_id UUID NOT NULL REFERENCES dataplane(id) ON DELETE CASCADE,
        organization_id UUID NOT NULL,
        diagnostic_kind VARCHAR(256) NOT NULL,
        target_id UUID NOT NULL,
        status VARCHAR(256) NOT NULL,
        checked_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        error_code VARCHAR(256),
        error_message TEXT,
        observation JSONB,
        CONSTRAINT dataplane_diagnostic_dataplane_kind_target_key UNIQUE (dataplane_id, diagnostic_kind, target_id)
      )
      """.trimIndent(),
    )
  }
}
