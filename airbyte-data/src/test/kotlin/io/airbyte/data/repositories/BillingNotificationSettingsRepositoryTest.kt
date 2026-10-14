/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.transaction.TransactionOperations
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.sql.Connection
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@MicronautTest
internal class BillingNotificationSettingsRepositoryTest : AbstractConfigRepositoryTest() {
  private lateinit var organizationId: UUID

  @Suppress("UNCHECKED_CAST")
  private val configTransactionOperations =
    context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>

  @BeforeEach
  fun setup() {
    organizationId = organizationRepository.save(Organization(name = "Airbyte Inc.", email = "contact@airbyte.io")).id!!
  }

  @AfterEach
  fun tearDown() {
    billingNotificationSettingsRepository.deleteAll()
    organizationRepository.deleteAll()
  }

  @Test
  fun `upsert inserts a row with both timestamps`() {
    billingNotificationSettingsRepository.upsert(organizationId, "alerts@example.com", BigDecimal("12.5"), BigDecimal("300"))

    val saved = billingNotificationSettingsRepository.findById(organizationId).get()
    saved.recipientEmail shouldBe "alerts@example.com"
    saved.syncCreditThreshold.compareTo(BigDecimal("12.5")) shouldBe 0
    saved.invoiceAmountThreshold.compareTo(BigDecimal("300")) shouldBe 0
    saved.createdAt.shouldNotBeNull()
    saved.updatedAt.shouldNotBeNull()
  }

  @Test
  fun `upsert twice keeps one row with the latest values and a newer updated_at`() {
    billingNotificationSettingsRepository.upsert(organizationId, "first@example.com", BigDecimal.ONE, BigDecimal.TEN)
    val first = billingNotificationSettingsRepository.findById(organizationId).get()

    billingNotificationSettingsRepository.upsert(organizationId, null, BigDecimal.ZERO, BigDecimal("42"))

    billingNotificationSettingsRepository.count() shouldBe 1L
    val second = billingNotificationSettingsRepository.findById(organizationId).get()
    second.recipientEmail shouldBe null
    second.syncCreditThreshold.compareTo(BigDecimal.ZERO) shouldBe 0
    second.invoiceAmountThreshold.compareTo(BigDecimal("42")) shouldBe 0
    second.createdAt shouldBe first.createdAt
    (second.updatedAt!! >= first.updatedAt!!) shouldBe true
  }

  @Test
  fun `null recipient and zero thresholds round-trip`() {
    billingNotificationSettingsRepository.upsert(organizationId, null, BigDecimal.ZERO, BigDecimal.ZERO)

    val saved = billingNotificationSettingsRepository.findById(organizationId).get()
    saved.recipientEmail shouldBe null
    saved.syncCreditThreshold.compareTo(BigDecimal.ZERO) shouldBe 0
    saved.invoiceAmountThreshold.compareTo(BigDecimal.ZERO) shouldBe 0
  }

  @Test
  fun `negative threshold is rejected by the database check`() {
    assertThrows<DataAccessException> {
      billingNotificationSettingsRepository.upsert(organizationId, null, BigDecimal("-1"), BigDecimal.ZERO)
    }
    billingNotificationSettingsRepository.count() shouldBe 0L
  }

  @Test
  fun `upsert holds the row lock until commit so a concurrent save waits and the read-back is this transaction's own write`() {
    val firstHasWritten = CountDownLatch(1)
    val allowFirstCommit = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val first =
        executor.submit(
          Callable {
            configTransactionOperations.executeWrite { _ ->
              billingNotificationSettingsRepository.upsert(organizationId, "first@example.com", BigDecimal.ONE, BigDecimal.ZERO)
              firstHasWritten.countDown()
              allowFirstCommit.await(10, TimeUnit.SECONDS) shouldBe true
              billingNotificationSettingsRepository.findById(organizationId).get().recipientEmail
            }
          },
        )
      firstHasWritten.await(10, TimeUnit.SECONDS) shouldBe true

      val second =
        executor.submit(
          Callable {
            configTransactionOperations.executeWrite { _ ->
              billingNotificationSettingsRepository.upsert(organizationId, "second@example.com", BigDecimal("2"), BigDecimal.ZERO)
              billingNotificationSettingsRepository.findById(organizationId).get().recipientEmail
            }
          },
        )
      assertThrows<TimeoutException> { second.get(500, TimeUnit.MILLISECONDS) }

      allowFirstCommit.countDown()
      first.get(10, TimeUnit.SECONDS) shouldBe "first@example.com"
      second.get(10, TimeUnit.SECONDS) shouldBe "second@example.com"
      billingNotificationSettingsRepository.findById(organizationId).get().recipientEmail shouldBe "second@example.com"
    } finally {
      allowFirstCommit.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `findById returns empty for an organization without settings`() {
    billingNotificationSettingsRepository.findById(UUID.randomUUID()).isPresent shouldBe false
  }
}
