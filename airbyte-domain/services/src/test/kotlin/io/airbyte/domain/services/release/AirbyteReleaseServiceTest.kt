/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.release

import io.airbyte.data.repositories.AirbyteReleaseRepository
import io.airbyte.domain.models.release.AirbyteRelease
import io.airbyte.domain.models.release.AirbyteReleaseConflictException
import io.airbyte.domain.models.release.InvalidAirbyteReleaseException
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import io.airbyte.data.repositories.entities.AirbyteRelease as AirbyteReleaseEntity

class AirbyteReleaseServiceTest {
  private val repository = mockk<AirbyteReleaseRepository>()
  private val service = AirbyteReleaseService(repository, ImmediateTransactionOperations())

  @Test
  fun `records a new release and returns the stored row`() {
    every { repository.insertIfAbsent(any(), RELEASE.version, RELEASE.sourceCommit, RELEASE.publishedAt) } returns 1L
    every { repository.findByVersion(RELEASE.version) } returns entity(RELEASE)

    assertEquals(RELEASE, service.record(RELEASE))
  }

  @Test
  fun `a retry with the same commit returns the original publication time`() {
    val retry = RELEASE.copy(publishedAt = RELEASE.publishedAt.plusHours(2))
    every { repository.insertIfAbsent(any(), any(), any(), any()) } returns 0L
    every { repository.findByVersion(RELEASE.version) } returns entity(RELEASE)

    assertEquals(RELEASE, service.record(retry))
  }

  @Test
  fun `the same version from a different commit is a conflict`() {
    every { repository.insertIfAbsent(any(), any(), any(), any()) } returns 0L
    every { repository.findByVersion(RELEASE.version) } returns entity(RELEASE)

    val error = assertThrows<AirbyteReleaseConflictException> { service.record(RELEASE.copy(sourceCommit = OTHER_COMMIT)) }

    assertEquals(RELEASE.version, error.version)
  }

  @ParameterizedTest
  @ValueSource(strings = ["", " ", " 2.1.0", "2.1.0\n"])
  fun `rejects a blank or padded version before touching the database`(version: String) {
    assertThrows<InvalidAirbyteReleaseException> { service.record(RELEASE.copy(version = version)) }

    verify(exactly = 0) { repository.insertIfAbsent(any(), any(), any(), any()) }
  }

  @Test
  fun `rejects a version longer than the column`() {
    assertThrows<InvalidAirbyteReleaseException> { service.record(RELEASE.copy(version = "1".repeat(257))) }

    verify(exactly = 0) { repository.insertIfAbsent(any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "",
      "release/2.1.0",
      "0123456",
      "0123456789ABCDEF0123456789ABCDEF01234567",
      "0123456789abcdef0123456789abcdef012345678",
    ],
  )
  fun `rejects a source commit that is not a full lowercase SHA`(sourceCommit: String) {
    assertThrows<InvalidAirbyteReleaseException> { service.record(RELEASE.copy(sourceCommit = sourceCommit)) }

    verify(exactly = 0) { repository.insertIfAbsent(any(), any(), any(), any()) }
  }

  private fun entity(release: AirbyteRelease) =
    AirbyteReleaseEntity(version = release.version, sourceCommit = release.sourceCommit, publishedAt = release.publishedAt)

  companion object {
    private const val OTHER_COMMIT = "89abcdef0123456789abcdef0123456789abcdef"
    private val RELEASE =
      AirbyteRelease(
        version = "2.1.0",
        sourceCommit = "0123456789abcdef0123456789abcdef01234567",
        publishedAt = OffsetDateTime.of(2026, 11, 6, 17, 30, 0, 0, ZoneOffset.UTC),
      )
  }
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
