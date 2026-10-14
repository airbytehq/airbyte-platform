/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.BillingNotificationSettings
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.math.BigDecimal
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface BillingNotificationSettingsRepository : PageableRepository<BillingNotificationSettings, UUID> {
  /**
   * One row per organization. A second save for the same organization replaces every field in
   * one statement, so two concurrent saves cannot race between an existence check and an insert.
   */
  @Query(
    """
    INSERT INTO billing_notification_settings
      (organization_id, recipient_email, sync_credit_threshold, invoice_amount_threshold, created_at, updated_at)
    VALUES (:organizationId, :recipientEmail, :syncCreditThreshold, :invoiceAmountThreshold, now(), now())
    ON CONFLICT (organization_id) DO UPDATE SET
      recipient_email = EXCLUDED.recipient_email,
      sync_credit_threshold = EXCLUDED.sync_credit_threshold,
      invoice_amount_threshold = EXCLUDED.invoice_amount_threshold,
      updated_at = now()
    """,
  )
  fun upsert(
    organizationId: UUID,
    recipientEmail: String?,
    syncCreditThreshold: BigDecimal,
    invoiceAmountThreshold: BigDecimal,
  )
}
