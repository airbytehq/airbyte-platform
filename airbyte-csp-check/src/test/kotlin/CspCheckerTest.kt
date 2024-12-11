package io.airbyte.commons.csp

import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.storage.StorageType
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CspCheckerTest {
  @Test
  fun `verify happy path`() {
    val storageType = StorageType.LOCAL

    val client: StorageClient =
      mockk {
        every { list(any()) } returns listOf(STORAGE_DOC_ID)
        every { write(any(), any()) } just Runs
        every { read(any()) } returns STORAGE_DOC_CONTENTS
        every { delete(any()) } returns true
        every { documentType } returnsMany storageDocTypes
        every { this@mockk.storageType } returns storageType
        every { bucketName } returns "test-bucket"
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = CspChecker(storageType = storageType, storageFactory = factory).check()

    assertEquals(StorageType.LOCAL, checkResult.storage.type)
    assertEquals(
      setOf(DocumentType.STATE, DocumentType.LOGS, DocumentType.WORKLOAD_OUTPUT, DocumentType.ACTIVITY_PAYLOADS, DocumentType.AUDIT_LOGS),
      checkResult.storage.buckets
        .map { it.documentType }
        .toSet(),
    )
    assertEquals(
      "test-bucket",
      checkResult.storage.buckets
        .first()
        .name,
    )
  }

  @Test
  fun `verify state list fails`() {
    val storageType = StorageType.LOCAL
    val msg = "failure message"

    val client: StorageClient =
      mockk {
        every { list(any()) } throws RuntimeException(msg)
        every { write(any(), any()) } just Runs
        every { read(any()) } returns STORAGE_DOC_CONTENTS
        every { delete(any()) } returns true
        every { documentType } returnsMany storageDocTypes
        every { this@mockk.storageType } returns storageType
        every { bucketName } returns "test-bucket"
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = CspChecker(storageType = storageType, storageFactory = factory).check()

    with(
      checkResult.storage.buckets
        .find { it.documentType == DocumentType.STATE }
        ?.results
        ?.get("list"),
    ) {
      assertEquals(STATUS_FAIL, this?.result)
      assertEquals(msg, this?.message)
    }
  }

  @Test
  fun `verify state write fails`() {
    val storageType = StorageType.LOCAL
    val msg = "failure message"

    val client: StorageClient =
      mockk {
        every { list(any()) } returns listOf(STORAGE_DOC_ID)
        every { write(any(), any()) } throws RuntimeException(msg)
        every { read(any()) } returns STORAGE_DOC_CONTENTS
        every { delete(any()) } returns true
        every { documentType } returnsMany storageDocTypes
        every { this@mockk.storageType } returns storageType
        every { bucketName } returns "test-bucket"
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = CspChecker(storageType = storageType, storageFactory = factory).check()

    with(
      checkResult.storage.buckets
        .find { it.documentType == DocumentType.STATE }
        ?.results
        ?.get("write"),
    ) {
      assertEquals(STATUS_FAIL, this?.result)
      assertEquals(msg, this?.message)
    }
  }

  @Test
  fun `verify state read fails`() {
    val storageType = StorageType.LOCAL
    val msg = "failure message"

    val client: StorageClient =
      mockk {
        every { list(any()) } returns listOf(STORAGE_DOC_ID)
        every { write(any(), any()) } just Runs
        every { read(any()) } throws RuntimeException(msg)
        every { delete(any()) } returns true
        every { documentType } returnsMany storageDocTypes
        every { this@mockk.storageType } returns storageType
        every { bucketName } returns "test-bucket"
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = CspChecker(storageType = storageType, storageFactory = factory).check()

    with(
      checkResult.storage.buckets
        .find { it.documentType == DocumentType.STATE }
        ?.results
        ?.get("read"),
    ) {
      assertEquals(STATUS_FAIL, this?.result)
      assertEquals(msg, this?.message)
    }
  }

  @Test
  fun `verify state delete fails`() {
    val storageType = StorageType.LOCAL
    val msg = "failure message"

    val client: StorageClient =
      mockk {
        every { list(any()) } returns listOf(STORAGE_DOC_ID)
        every { write(any(), any()) } just Runs
        every { read(any()) } returns STORAGE_DOC_CONTENTS
        every { delete(any()) } throws RuntimeException(msg)
        every { documentType } returnsMany storageDocTypes
        every { this@mockk.storageType } returns storageType
        every { bucketName } returns "test-bucket"
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = CspChecker(storageType = storageType, storageFactory = factory).check()

    with(
      checkResult.storage.buckets
        .find { it.documentType == DocumentType.STATE }
        ?.results
        ?.get("delete"),
    ) {
      assertEquals(STATUS_FAIL, this?.result)
      assertEquals(msg, this?.message)
    }
  }
}
