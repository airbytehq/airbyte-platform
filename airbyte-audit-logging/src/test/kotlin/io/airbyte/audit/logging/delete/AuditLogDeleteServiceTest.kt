/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.delete

import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class AuditLogDeleteServiceTest {
  private lateinit var storageClient: StorageClient
  private lateinit var service: AuditLogDeleteService

  private val organizationId: UUID = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    storageClient = mockk()
    service = AuditLogDeleteService(BASE_STORAGE_ID, storageClient)
  }

  @Test
  fun `deletes every file under the organization prefix and returns the count`() {
    val fileOne = "$BASE_STORAGE_ID/$organizationId/2026-08-14/20260814100000_host_abc.json"
    val fileTwo = "$BASE_STORAGE_ID/$organizationId/2026-08-15/20260815100000_host_def.json"
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } returns
      listOf("$DOCUMENT_TYPE_PREFIX/$fileOne", "$DOCUMENT_TYPE_PREFIX/$fileTwo")
    every { storageClient.delete(fileOne) } returns true
    every { storageClient.delete(fileTwo) } returns true

    val deleted = service.deleteAuditLogsByOrganizationId(organizationId)

    assertEquals(2, deleted)
    verifyOrder {
      storageClient.delete(fileOne)
      storageClient.delete(fileTwo)
    }
  }

  @Test
  fun `recurses into date partition prefix entries`() {
    val partition = "$BASE_STORAGE_ID/$organizationId/2026-08-14"
    val file = "$partition/20260814100000_host_abc.json"
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } returns listOf("$DOCUMENT_TYPE_PREFIX/$partition/")
    every { storageClient.list(partition) } returns listOf("$DOCUMENT_TYPE_PREFIX/$file")
    every { storageClient.delete(file) } returns true

    val deleted = service.deleteAuditLogsByOrganizationId(organizationId)

    assertEquals(1, deleted)
    verify(exactly = 1) { storageClient.delete(file) }
  }

  @Test
  fun `accepts ids returned relative to the document type prefix`() {
    val file = "$BASE_STORAGE_ID/$organizationId/2026-08-14/20260814100000_host_abc.json"
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } returns listOf(file)
    every { storageClient.delete(file) } returns true

    val deleted = service.deleteAuditLogsByOrganizationId(organizationId)

    assertEquals(1, deleted)
    verify(exactly = 1) { storageClient.delete(file) }
  }

  @Test
  fun `returns zero when nothing is stored for the organization`() {
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } returns emptyList()

    assertEquals(0, service.deleteAuditLogsByOrganizationId(organizationId))
    verify(exactly = 0) { storageClient.delete(any()) }
  }

  @Test
  fun `only ever lists and deletes under the organization prefix`() {
    val file = "$BASE_STORAGE_ID/$organizationId/2026-08-14/20260814100000_host_abc.json"
    val otherOrgFile = "$BASE_STORAGE_ID/${UUID.randomUUID()}/2026-08-14/20260814100000_host_abc.json"
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } returns listOf("$DOCUMENT_TYPE_PREFIX/$file")
    every { storageClient.delete(file) } returns true

    service.deleteAuditLogsByOrganizationId(organizationId)

    verify(exactly = 1) { storageClient.list(match { it.startsWith("$BASE_STORAGE_ID/$organizationId") }) }
    verify(exactly = 1) { storageClient.delete(match { it.startsWith("$BASE_STORAGE_ID/$organizationId/") }) }
    verify(exactly = 0) { storageClient.delete(otherOrgFile) }
  }

  @Test
  fun `does not count files that no longer exist at delete time`() {
    val vanished = "$BASE_STORAGE_ID/$organizationId/2026-08-14/20260814100000_host_abc.json"
    val existing = "$BASE_STORAGE_ID/$organizationId/2026-08-15/20260815100000_host_def.json"
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } returns
      listOf("$DOCUMENT_TYPE_PREFIX/$vanished", "$DOCUMENT_TYPE_PREFIX/$existing")
    every { storageClient.delete(vanished) } returns false
    every { storageClient.delete(existing) } returns true

    assertEquals(1, service.deleteAuditLogsByOrganizationId(organizationId))
  }

  @Test
  fun `returns zero without touching storage when the audit log bucket is not configured`() {
    val unconfiguredService = AuditLogDeleteService(" ", storageClient)

    assertEquals(0, unconfiguredService.deleteAuditLogsByOrganizationId(organizationId))
    verify(exactly = 0) { storageClient.list(any()) }
    verify(exactly = 0) { storageClient.delete(any()) }
  }

  @Test
  fun `propagates deletion failures`() {
    every { storageClient.list("$BASE_STORAGE_ID/$organizationId") } throws RuntimeException("list boom")

    assertThrows<RuntimeException> { service.deleteAuditLogsByOrganizationId(organizationId) }
  }

  companion object {
    private const val BASE_STORAGE_ID = "audit-logs"
    private val DOCUMENT_TYPE_PREFIX = DocumentType.AUDIT_LOGS.prefix.toString()
  }
}
