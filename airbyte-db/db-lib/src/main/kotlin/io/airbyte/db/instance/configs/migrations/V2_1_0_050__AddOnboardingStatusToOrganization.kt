/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Adds the organization's status in the Cloud onboarding flow. NULL means the organization was never
 * in the flow, so existing organizations need no backfill. The column is varchar, not an enum type,
 * because the flow is an experiment and its steps may change. The onboarding API validates the
 * values.
 *
 * The schema is reversible with a follow-up migration that drops the column. Flyway migrations are
 * not rolled back in place.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_050__AddOnboardingStatusToOrganization : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addOnboardingStatusColumn(ctx)
  }

  companion object {
    fun addOnboardingStatusColumn(ctx: DSLContext) {
      ctx.execute("ALTER TABLE $ORGANIZATION_TABLE ADD COLUMN onboarding_status varchar")
    }
  }
}
