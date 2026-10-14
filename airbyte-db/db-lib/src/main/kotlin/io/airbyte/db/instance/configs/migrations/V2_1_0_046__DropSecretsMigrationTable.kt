/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Drops the `secrets_migration` table. It was created by hand in Cloud, outside Flyway, to track
 * progress of the legacy secrets migration run by `tools/secrets_migration`. OSS deployments never
 * had it, so the drop is a no-op there.
 *
 * Not reversible: the dropped rows are lost. Restoring the table requires a new forward migration
 * that recreates it empty; never edit or remove an applied migration.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_046__DropSecretsMigrationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute("DROP TABLE IF EXISTS secrets_migration")
  }
}
