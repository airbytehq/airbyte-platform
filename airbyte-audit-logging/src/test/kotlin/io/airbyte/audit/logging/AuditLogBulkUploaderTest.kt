/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.StorageClient
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class AuditLogBulkUploaderTest {
  private lateinit var storageClient: StorageClient
  private lateinit var uploader: AuditLogBulkUploader

  @BeforeEach
  fun setUp() {
    storageClient = mockk()
    uploader = AuditLogBulkUploader(BASE_STORAGE_ID, storageClient)
  }

  @Test
  fun `writes one file per organization partition`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val entriesOrgOne = listOf(entry(organizationId), entry(organizationId))
    val entriesOrgTwo = listOf(entry(otherOrganizationId))
    (entriesOrgOne + entriesOrgTwo).forEach { uploader.buffer.add(it) }

    val writes = captureWrites()
    uploader.upload()

    assertEquals(2, writes.size)
    val byOrganization = writes.associateBy { organizationSegment(it.first) }
    assertPayloadEntryIds(byOrganization.getValue(organizationId.toString()).second, entriesOrgOne)
    assertPayloadEntryIds(byOrganization.getValue(otherOrganizationId.toString()).second, entriesOrgTwo)
  }

  @Test
  fun `writes one file per date partition for entries spanning midnight`() {
    val organizationId = UUID.randomUUID()
    val beforeMidnight = entry(organizationId, timestamp = Instant.parse("2026-08-14T23:59:59Z").toEpochMilli())
    val afterMidnight = entry(organizationId, timestamp = Instant.parse("2026-08-15T00:00:01Z").toEpochMilli())
    uploader.buffer.add(beforeMidnight)
    uploader.buffer.add(afterMidnight)

    val writes = captureWrites()
    uploader.upload()

    assertEquals(2, writes.size)
    val beforeWrite = writes.single { it.first.contains("/$organizationId/2026-08-14/") }
    val afterWrite = writes.single { it.first.contains("/$organizationId/2026-08-15/") }
    assertPayloadEntryIds(beforeWrite.second, listOf(beforeMidnight))
    assertPayloadEntryIds(afterWrite.second, listOf(afterMidnight))
  }

  @Test
  fun `file id uses organization and date segments under the base storage id`() {
    val organizationId = UUID.randomUUID()
    uploader.buffer.add(entry(organizationId, timestamp = Instant.parse("2026-08-14T12:00:00Z").toEpochMilli()))

    val writes = captureWrites()
    uploader.upload()

    val (id, _) = writes.single()
    assertTrue(id.startsWith("$BASE_STORAGE_ID/$organizationId/2026-08-14/"), "unexpected file id: $id")
    assertTrue(id.endsWith(".json"), "unexpected file id: $id")
  }

  @Test
  fun `entries without an organization are written under the unknown partition`() {
    uploader.buffer.add(entry(organizationId = null))

    val writes = captureWrites()
    uploader.upload()

    val (id, _) = writes.single()
    assertTrue(id.contains("/${AuditLogBulkUploader.UNKNOWN_ORGANIZATION}/"), "unexpected file id: $id")
  }

  @Test
  fun `upload is a no-op when the buffer is empty`() {
    uploader.upload()

    verify(exactly = 0) { storageClient.write(any(), any()) }
  }

  @Test
  fun `a failing partition write does not fail the remaining partitions`() {
    val failingOrganizationId = UUID.randomUUID()
    val survivingOrganizationId = UUID.randomUUID()
    uploader.buffer.add(entry(failingOrganizationId))
    uploader.buffer.add(entry(survivingOrganizationId))

    every { storageClient.write(match { it.contains("/$failingOrganizationId/") }, any()) } throws RuntimeException("storage down")
    every { storageClient.write(match { it.contains("/$survivingOrganizationId/") }, any()) } just Runs

    uploader.upload()

    verify(exactly = 1) { storageClient.write(match { it.contains("/$survivingOrganizationId/") }, any()) }
    assertTrue(uploader.buffer.isEmpty())
  }

  private fun entry(
    organizationId: UUID?,
    timestamp: Long = Instant.parse("2026-08-14T12:00:00Z").toEpochMilli(),
  ): AuditLogEntry =
    AuditLogEntry(
      id = UUID.randomUUID(),
      timestamp = timestamp,
      operation = "testOperation",
      success = true,
      organizationId = organizationId,
      workspaceId = UUID.randomUUID(),
    )

  private fun captureWrites(): MutableList<Pair<String, String>> {
    val writes = mutableListOf<Pair<String, String>>()
    every { storageClient.write(any(), any()) } answers {
      writes.add(firstArg<String>() to secondArg<String>())
    }
    return writes
  }

  private fun organizationSegment(fileId: String): String = fileId.split("/")[1]

  private fun assertPayloadEntryIds(
    document: String,
    expectedEntries: List<AuditLogEntry>,
  ) {
    val payload = Jsons.deserialize(document)
    assertEquals(expectedEntries.size, payload.size())
    assertEquals(
      expectedEntries.map { it.id.toString() }.toSet(),
      payload.map { it.get("id").asText() }.toSet(),
    )
  }

  companion object {
    private const val BASE_STORAGE_ID = "audit-logging"
  }
}
