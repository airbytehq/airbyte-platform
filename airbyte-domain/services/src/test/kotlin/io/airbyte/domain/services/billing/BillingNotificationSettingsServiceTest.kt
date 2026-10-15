/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.billing

import io.airbyte.data.repositories.BillingNotificationSettingsRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.billing.BillingNotificationSettingsValidationException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.Connection
import java.util.Optional
import java.util.UUID
import io.airbyte.data.repositories.entities.BillingNotificationSettings as BillingNotificationSettingsEntity

internal class BillingNotificationSettingsServiceTest {
  private val repository: BillingNotificationSettingsRepository = mockk(relaxed = true)
  private val organizationRepository: OrganizationRepository = mockk()
  private val service = BillingNotificationSettingsService(repository, organizationRepository, ImmediateTransactionOperations())
  private val organizationId = OrganizationId(UUID.randomUUID())

  @Test
  fun `get returns the unconfigured defaults when the organization has no row`() {
    every { repository.findById(organizationId.value) } returns Optional.empty()

    val settings = service.get(organizationId)

    settings.organizationId shouldBe organizationId
    settings.configured shouldBe false
    settings.recipientEmail shouldBe null
    settings.syncCreditThreshold shouldBe BigDecimal.ZERO
    settings.invoiceAmountThreshold shouldBe BigDecimal.ZERO
  }

  @Test
  fun `get maps the stored row`() {
    every { repository.findById(organizationId.value) } returns
      Optional.of(entity(email = "alerts@example.com", sync = "5", invoice = "250"))

    val settings = service.get(organizationId)

    settings.configured shouldBe true
    settings.recipientEmail shouldBe "alerts@example.com"
    settings.syncCreditThreshold shouldBe BigDecimal("5")
    settings.invoiceAmountThreshold shouldBe BigDecimal("250")
  }

  @Test
  fun `save trims the email, upserts and returns the stored row`() {
    every { repository.findById(organizationId.value) } returns
      Optional.of(entity(email = "alerts@example.com", sync = "7", invoice = "0"))

    val saved = service.save(organizationId, " alerts@example.com ", BigDecimal("7"), BigDecimal.ZERO)

    verify(exactly = 1) { repository.upsert(organizationId.value, "alerts@example.com", BigDecimal("7"), BigDecimal.ZERO) }
    saved.recipientEmail shouldBe "alerts@example.com"
  }

  @Test
  fun `save stores a blank email as null`() {
    every { repository.findById(organizationId.value) } returns Optional.of(entity(email = null, sync = "0", invoice = "0"))

    service.save(organizationId, "   ", BigDecimal.ZERO, BigDecimal.ZERO)

    verify(exactly = 1) { repository.upsert(organizationId.value, null, BigDecimal.ZERO, BigDecimal.ZERO) }
  }

  @Test
  fun `save stores a null email as null`() {
    every { repository.findById(organizationId.value) } returns Optional.of(entity(email = null, sync = "1", invoice = "1"))

    service.save(organizationId, null, BigDecimal.ONE, BigDecimal.ONE)

    verify(exactly = 1) { repository.upsert(organizationId.value, null, BigDecimal.ONE, BigDecimal.ONE) }
  }

  @Test
  fun `save accepts an email at the column limit`() {
    val email = "a".repeat(256 - "@example.com".length) + "@example.com"
    email.length shouldBe 256
    every { repository.findById(organizationId.value) } returns Optional.of(entity(email = email, sync = "0", invoice = "0"))

    service.save(organizationId, email, BigDecimal.ZERO, BigDecimal.ZERO)

    verify(exactly = 1) { repository.upsert(organizationId.value, email, BigDecimal.ZERO, BigDecimal.ZERO) }
  }

  @Test
  fun `save rejects an email longer than the column before writing`() {
    val email = "a".repeat(257 - "@example.com".length) + "@example.com"
    email.length shouldBe 257

    shouldThrow<BillingNotificationSettingsValidationException> {
      service.save(organizationId, email, BigDecimal.ZERO, BigDecimal.ZERO)
    }
    verify(exactly = 0) { repository.upsert(any(), any(), any(), any()) }
  }

  @Test
  fun `save rejects a malformed email before writing`() {
    shouldThrow<BillingNotificationSettingsValidationException> {
      service.save(organizationId, "not-an-email", BigDecimal.ZERO, BigDecimal.ZERO)
    }
    verify(exactly = 0) { repository.upsert(any(), any(), any(), any()) }
  }

  @Test
  fun `save rejects a negative sync credit threshold before writing`() {
    shouldThrow<BillingNotificationSettingsValidationException> {
      service.save(organizationId, null, BigDecimal("-1"), BigDecimal.ZERO)
    }
    verify(exactly = 0) { repository.upsert(any(), any(), any(), any()) }
  }

  @Test
  fun `save rejects a negative invoice amount threshold before writing`() {
    shouldThrow<BillingNotificationSettingsValidationException> {
      service.save(organizationId, null, BigDecimal.ZERO, BigDecimal("-0.01"))
    }
    verify(exactly = 0) { repository.upsert(any(), any(), any(), any()) }
  }

  @Test
  fun `save fails loudly when the row is missing after the upsert`() {
    every { repository.findById(organizationId.value) } returns Optional.empty()

    shouldThrow<IllegalStateException> {
      service.save(organizationId, null, BigDecimal.ZERO, BigDecimal.ZERO)
    }
  }

  @Test
  fun `organizationName returns the live organization's name`() {
    every { organizationRepository.findByIdAndTombstoneFalse(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "Example & Partners", email = "owner@example.com"))

    service.organizationName(organizationId) shouldBe "Example & Partners"
  }

  @Test
  fun `organizationName fails loudly when the organization is missing or deleted`() {
    every { organizationRepository.findByIdAndTombstoneFalse(organizationId.value) } returns Optional.empty()

    shouldThrow<IllegalStateException> { service.organizationName(organizationId) }
  }

  private fun entity(
    email: String?,
    sync: String,
    invoice: String,
  ): BillingNotificationSettingsEntity =
    BillingNotificationSettingsEntity(
      organizationId = organizationId.value,
      recipientEmail = email,
      syncCreditThreshold = BigDecimal(sync),
      invoiceAmountThreshold = BigDecimal(invoice),
    )
}

private class ImmediateTransactionOperations : TransactionOperations<Connection> {
  override fun getConnection(): Connection = mockk(relaxed = true)

  override fun hasConnection(): Boolean = true

  override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

  override fun <R : Any?> execute(
    definition: TransactionDefinition,
    callback: TransactionCallback<Connection, R>,
  ): R = callback.call(mockk(relaxed = true))
}
