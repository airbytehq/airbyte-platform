/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.DataplaneDiagnostic
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest
class DataplaneDiagnosticRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making a dataplane
      jooqDslContext
        .alterTable(Tables.DATAPLANE_DIAGNOSTIC)
        .dropForeignKey(Keys.DATAPLANE_DIAGNOSTIC__DATAPLANE_DIAGNOSTIC_DATAPLANE_ID_FKEY.constraint())
        .execute()
    }

    private val CHECKED_AT = OffsetDateTime.of(2026, 11, 3, 10, 0, 0, 0, ZoneOffset.UTC)
  }

  @AfterEach
  fun cleanup() {
    jooqDslContext.deleteFrom(Tables.DATAPLANE_DIAGNOSTIC).execute()
  }

  private val organizationId = UUID.randomUUID()
  private val dataplaneId = UUID.randomUUID()
  private val targetId = UUID.randomUUID()

  @Test
  fun `upsertLatest inserts a new result`() {
    val expected = passedResult()

    upsert(expected)

    assertEquals(listOf(expected), findSaved(listOf(dataplaneId)))
  }

  @Test
  fun `upsertLatest replaces the saved result with a newer one`() {
    upsert(passedResult())
    val newer =
      passedResult().copy(
        status = "FAILED",
        checkedAt = CHECKED_AT.plusMinutes(5),
        errorCode = "ACCESS_DENIED",
        errorMessage = "Access denied",
      )

    upsert(newer)

    assertEquals(listOf(newer), findSaved(listOf(dataplaneId)))
  }

  @Test
  fun `upsertLatest ignores a result older than the saved one`() {
    val saved = passedResult()
    upsert(saved)

    upsert(saved.copy(status = "FAILED", checkedAt = CHECKED_AT.minusMinutes(5), errorMessage = "Access denied"))

    assertEquals(listOf(saved), findSaved(listOf(dataplaneId)))
  }

  @Test
  fun `upsertLatest keeps one row per target`() {
    val first = passedResult()
    val second = passedResult().copy(targetId = UUID.randomUUID())

    upsert(first)
    upsert(second)

    assertEquals(listOf(first, second).sortedBy { it.targetId }, findSaved(listOf(dataplaneId)).sortedBy { it.targetId })
  }

  @Test
  fun `findByOrganizationIdAndDataplaneIdIn returns only the organization's requested dataplanes`() {
    val requested = passedResult()
    upsert(requested)
    upsert(passedResult().copy(dataplaneId = UUID.randomUUID()))
    val otherOrganizationDataplane = passedResult().copy(dataplaneId = UUID.randomUUID(), organizationId = UUID.randomUUID())
    upsert(otherOrganizationDataplane)

    assertEquals(listOf(requested), findSaved(listOf(dataplaneId, otherOrganizationDataplane.dataplaneId)))
  }

  @Test
  fun `deleteUnreportedTargets deletes an older result for a target that is no longer reported`() {
    val reported = passedResult()
    upsert(reported)
    upsert(passedResult().copy(targetId = UUID.randomUUID()))

    val deleted = deleteUnreported(listOf(reported.targetId), CHECKED_AT.plusMinutes(5))

    assertEquals(1, deleted)
    assertEquals(listOf(reported), findSaved(listOf(dataplaneId)))
  }

  @Test
  fun `deleteUnreportedTargets keeps an unreported result that is not older than the reported ones`() {
    val unreported = passedResult().copy(targetId = UUID.randomUUID(), checkedAt = CHECKED_AT.plusMinutes(5))
    upsert(unreported)

    val deleted = deleteUnreported(listOf(targetId), CHECKED_AT.plusMinutes(5))

    assertEquals(0, deleted)
    assertEquals(listOf(unreported), findSaved(listOf(dataplaneId)))
  }

  @Test
  fun `deleteUnreportedTargets leaves other kinds and other dataplanes alone`() {
    val otherKind = passedResult().copy(diagnosticKind = "STORAGE_REACHABILITY", targetId = UUID.randomUUID())
    val otherDataplane = passedResult().copy(dataplaneId = UUID.randomUUID(), targetId = UUID.randomUUID())
    upsert(otherKind)
    upsert(otherDataplane)

    val deleted = deleteUnreported(listOf(targetId), CHECKED_AT.plusMinutes(5))

    assertEquals(0, deleted)
    assertEquals(
      listOf(otherKind, otherDataplane).sortedBy { it.targetId },
      findSaved(listOf(dataplaneId, otherDataplane.dataplaneId)).sortedBy { it.targetId },
    )
  }

  @Test
  fun `upsertLatest fails for an error code longer than 256 characters`() {
    val longestAllowed = passedResult().copy(status = "FAILED", errorCode = "E".repeat(256), errorMessage = "Access denied")
    upsert(longestAllowed)

    assertThrows<DataAccessException> {
      upsert(longestAllowed.copy(targetId = UUID.randomUUID(), errorCode = "E".repeat(257)))
    }
    assertEquals(listOf(longestAllowed), findSaved(listOf(dataplaneId)))
  }

  private fun passedResult() =
    DataplaneDiagnostic(
      dataplaneId = dataplaneId,
      organizationId = organizationId,
      diagnosticKind = "SECRET_STORAGE",
      targetId = targetId,
      status = "PASSED",
      checkedAt = CHECKED_AT,
      observation = Jsons.jsonNode(mapOf("scope_type" to "workspace", "scope_id" to UUID.randomUUID().toString())),
    )

  private fun upsert(diagnostic: DataplaneDiagnostic) =
    dataplaneDiagnosticRepository.upsertLatest(
      dataplaneId = diagnostic.dataplaneId,
      organizationId = diagnostic.organizationId,
      diagnosticKind = diagnostic.diagnosticKind,
      targetId = diagnostic.targetId,
      status = diagnostic.status,
      checkedAt = diagnostic.checkedAt,
      errorCode = diagnostic.errorCode,
      errorMessage = diagnostic.errorMessage,
      observation = diagnostic.observation?.let { Jsons.serialize(it) },
    )

  private fun deleteUnreported(
    reportedTargetIds: List<UUID>,
    checkedBefore: OffsetDateTime,
  ) = dataplaneDiagnosticRepository.deleteUnreportedTargets(dataplaneId, "SECRET_STORAGE", reportedTargetIds, checkedBefore)

  // Clears the fields the database sets so results compare as whole values.
  private fun findSaved(dataplaneIds: List<UUID>) =
    dataplaneDiagnosticRepository
      .findByOrganizationIdAndDataplaneIdIn(organizationId, dataplaneIds)
      .map { it.copy(id = null, createdAt = null, updatedAt = null, checkedAt = it.checkedAt.withOffsetSameInstant(ZoneOffset.UTC)) }
}
