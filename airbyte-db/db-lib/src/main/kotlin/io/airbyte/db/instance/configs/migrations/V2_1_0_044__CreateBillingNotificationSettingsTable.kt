/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.BILLING_NOTIFICATION_SETTINGS_TABLE
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Creates the table holding an organization's billing notification settings.
 *
 * One row per organization. Both thresholds default to 0, and 0 means that alert is off, so a new
 * row sends nothing until a user sets a positive value. Thresholds are NUMERIC because credits and
 * invoice amounts are fractional and are compared exactly against usage. A null recipient_email
 * means the alert goes to the organization's payment-provider email.
 *
 * The schema is reversible with a follow-up migration that drops the table. Flyway migrations are
 * not rolled back in place.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_044__CreateBillingNotificationSettingsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute(
      """
      CREATE TABLE $BILLING_NOTIFICATION_SETTINGS_TABLE (
        organization_id UUID PRIMARY KEY REFERENCES $ORGANIZATION_TABLE(id),
        recipient_email VARCHAR(256),
        sync_credit_threshold NUMERIC NOT NULL DEFAULT 0,
        invoice_amount_threshold NUMERIC NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT billing_notification_settings_sync_credit_threshold_check CHECK (sync_credit_threshold >= 0),
        CONSTRAINT billing_notification_settings_invoice_amount_threshold_check CHECK (invoice_amount_threshold >= 0)
      )
      """.trimIndent(),
    )
  }
}
