/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Adds organization-level Semantic Search enablement, disabled for existing and new organizations.
 *
 * Application rollback can leave this additive column unused. Removing the column requires a new
 * forward migration and loses stored enablement values; never edit or remove an applied migration.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_043__AddSemanticSearchEnabledToOrganization : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute("ALTER TABLE $ORGANIZATION_TABLE ADD COLUMN semantic_search_enabled boolean NOT NULL DEFAULT false")
  }
}
