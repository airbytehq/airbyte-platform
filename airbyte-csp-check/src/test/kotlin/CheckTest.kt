package io.airbyte.commons.env

import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.storage.StorageType
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CheckTest {
  @Test
  fun `verify happy path`() {
    val storageType = StorageType.LOCAL

    val client: StorageClient =
      mockk {
        every { list(any()) } returns listOf(STORAGE_DOC_ID)
        every { write(any(), any()) } just Runs
        every { read(any()) } returns STORAGE_DOC_CONTENTS
        every { delete(any()) } returns true
        every { documentType() } returnsMany storageDocTypes
        every { storageType() } returns storageType
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = Check(storageType = storageType, storageFactory = factory).check()

    assertEquals(StorageType.LOCAL, checkResult.storage.type)
    with(checkResult.storage.results) {
      assertEquals(4, size)
      // 4 storage clients tested
      assertEquals(setOf("STATE", "LOGS", "WORKLOAD_OUTPUT", "ACTIVITY_PAYLOADS"), keys)
      // each client should have four tests
      // each test should be green
      forEach { (_, v) ->
        assertEquals(setOf("write", "read", "list", "delete"), v.keys)
        v.forEach {
          assertEquals(STATUS_GREEN, it.value.result)
        }
      }
    }
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
        every { documentType() } returnsMany storageDocTypes
        every { storageType() } returns storageType
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = Check(storageType = storageType, storageFactory = factory).check()

    with(checkResult.storage.results["STATE"]?.get("list")) {
      assertEquals(STATUS_RED, this?.result)
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
        every { documentType() } returnsMany storageDocTypes
        every { storageType() } returns storageType
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = Check(storageType = storageType, storageFactory = factory).check()

    with(checkResult.storage.results["STATE"]?.get("write")) {
      assertEquals(STATUS_RED, this?.result)
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
        every { documentType() } returnsMany storageDocTypes
        every { storageType() } returns storageType
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = Check(storageType = storageType, storageFactory = factory).check()

    with(checkResult.storage.results["STATE"]?.get("read")) {
      assertEquals(STATUS_RED, this?.result)
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
        every { documentType() } returnsMany storageDocTypes
        every { storageType() } returns storageType
        every { key(any()) } answers { callOriginal() }
      }

    val factory: StorageClientFactory =
      mockk {
        every { create(any()) } returns client
      }

    val checkResult = Check(storageType = storageType, storageFactory = factory).check()

    with(checkResult.storage.results["STATE"]?.get("delete")) {
      assertEquals(STATUS_RED, this?.result)
      assertEquals(msg, this?.message)
    }
  }
}
