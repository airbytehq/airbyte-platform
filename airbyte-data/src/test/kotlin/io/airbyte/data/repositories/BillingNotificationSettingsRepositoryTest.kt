/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.transaction.TransactionOperations
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
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
    assertEquals("alerts@example.com", saved.recipientEmail)
    assertEquals(0, saved.syncCreditThreshold.compareTo(BigDecimal("12.5")))
    assertEquals(0, saved.invoiceAmountThreshold.compareTo(BigDecimal("300")))
    assertNotNull(saved.createdAt)
    assertNotNull(saved.updatedAt)
  }

  @Test
  fun `upsert twice keeps one row with the latest values and a newer updated_at`() {
    billingNotificationSettingsRepository.upsert(organizationId, "first@example.com", BigDecimal.ONE, BigDecimal.TEN)
    val first = billingNotificationSettingsRepository.findById(organizationId).get()

    billingNotificationSettingsRepository.upsert(organizationId, null, BigDecimal.ZERO, BigDecimal("42"))

    assertEquals(1L, billingNotificationSettingsRepository.count())
    val second = billingNotificationSettingsRepository.findById(organizationId).get()
    assertNull(second.recipientEmail)
    assertEquals(0, second.syncCreditThreshold.compareTo(BigDecimal.ZERO))
    assertEquals(0, second.invoiceAmountThreshold.compareTo(BigDecimal("42")))
    assertEquals(first.createdAt, second.createdAt)
    assertTrue(second.updatedAt!! >= first.updatedAt!!)
  }

  @Test
  fun `null recipient and zero thresholds round-trip`() {
    billingNotificationSettingsRepository.upsert(organizationId, null, BigDecimal.ZERO, BigDecimal.ZERO)

    val saved = billingNotificationSettingsRepository.findById(organizationId).get()
    assertNull(saved.recipientEmail)
    assertEquals(0, saved.syncCreditThreshold.compareTo(BigDecimal.ZERO))
    assertEquals(0, saved.invoiceAmountThreshold.compareTo(BigDecimal.ZERO))
  }

  @Test
  fun `negative threshold is rejected by the database check`() {
    assertThrows<DataAccessException> {
      billingNotificationSettingsRepository.upsert(organizationId, null, BigDecimal("-1"), BigDecimal.ZERO)
    }
    assertEquals(0L, billingNotificationSettingsRepository.count())
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
              assertTrue(allowFirstCommit.await(10, TimeUnit.SECONDS))
              billingNotificationSettingsRepository.findById(organizationId).get().recipientEmail
            }
          },
        )
      assertTrue(firstHasWritten.await(10, TimeUnit.SECONDS))

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
      assertEquals("first@example.com", first.get(10, TimeUnit.SECONDS))
      assertEquals("second@example.com", second.get(10, TimeUnit.SECONDS))
      assertEquals("second@example.com", billingNotificationSettingsRepository.findById(organizationId).get().recipientEmail)
    } finally {
      allowFirstCommit.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `findById returns empty for an organization without settings`() {
    assertFalse(billingNotificationSettingsRepository.findById(UUID.randomUUID()).isPresent)
  }
}
