/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.billing

import io.airbyte.data.repositories.BillingNotificationSettingsRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.billing.BillingNotificationSettings
import io.airbyte.domain.models.billing.BillingNotificationSettingsValidationException
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.math.BigDecimal
import java.sql.Connection
import io.airbyte.data.repositories.entities.BillingNotificationSettings as BillingNotificationSettingsEntity

/** Matches the `recipient_email varchar(256)` column. */
private const val RECIPIENT_EMAIL_MAX_LENGTH = 256

private val EMAIL_SHAPE = Regex("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$")

@Singleton
open class BillingNotificationSettingsService(
  private val repository: BillingNotificationSettingsRepository,
  private val organizationRepository: OrganizationRepository,
  @param:Named("config") private val configTransactionOperations: TransactionOperations<Connection>,
) {
  open fun get(organizationId: OrganizationId): BillingNotificationSettings =
    repository.findById(organizationId.value).orElse(null)?.toDomain()
      ?: BillingNotificationSettings.unconfigured(organizationId)

  /**
   * Creates or replaces the organization's single row and returns what was stored.
   *
   * The upsert and the read-back share one transaction. The upsert holds the row lock until
   * commit, so a concurrent save waits and the read returns this call's values, not the other's.
   *
   * @throws BillingNotificationSettingsValidationException when a threshold is negative or the
   * email is malformed or longer than the column allows.
   */
  open fun save(
    organizationId: OrganizationId,
    recipientEmail: String?,
    syncCreditThreshold: BigDecimal,
    invoiceAmountThreshold: BigDecimal,
  ): BillingNotificationSettings {
    val normalizedEmail = recipientEmail?.trim()?.ifEmpty { null }
    requireNonNegative("syncCreditThreshold", syncCreditThreshold)
    requireNonNegative("invoiceAmountThreshold", invoiceAmountThreshold)
    if (normalizedEmail != null) {
      if (normalizedEmail.length > RECIPIENT_EMAIL_MAX_LENGTH) {
        throw BillingNotificationSettingsValidationException("recipientEmail must be at most $RECIPIENT_EMAIL_MAX_LENGTH characters")
      }
      if (!EMAIL_SHAPE.matches(normalizedEmail)) {
        throw BillingNotificationSettingsValidationException("recipientEmail is not a valid email address")
      }
    }

    return configTransactionOperations.executeWrite { _ ->
      repository.upsert(
        organizationId = organizationId.value,
        recipientEmail = normalizedEmail,
        syncCreditThreshold = syncCreditThreshold,
        invoiceAmountThreshold = invoiceAmountThreshold,
      )
      repository
        .findById(organizationId.value)
        .orElseThrow {
          IllegalStateException(
            "billing_notification_settings row missing right after upsert for organization ${organizationId.value}",
          )
        }.toDomain()
    }
  }

  /**
   * Name shown in the confirmation email. The settings row references the organization, so a
   * missing or deleted organization after a save is an invariant violation, not a normal outcome.
   */
  open fun organizationName(organizationId: OrganizationId): String =
    organizationRepository
      .findByIdAndTombstoneFalse(organizationId.value)
      .orElseThrow { IllegalStateException("Organization ${organizationId.value} not found") }
      .name

  private fun requireNonNegative(
    field: String,
    value: BigDecimal,
  ) {
    if (value < BigDecimal.ZERO) {
      throw BillingNotificationSettingsValidationException("$field must be 0 or greater")
    }
  }

  private fun BillingNotificationSettingsEntity.toDomain(): BillingNotificationSettings =
    BillingNotificationSettings(
      organizationId = OrganizationId(organizationId),
      configured = true,
      recipientEmail = recipientEmail,
      syncCreditThreshold = syncCreditThreshold,
      invoiceAmountThreshold = invoiceAmountThreshold,
    )
}
