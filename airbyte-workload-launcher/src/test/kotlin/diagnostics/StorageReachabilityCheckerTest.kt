/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

internal class StorageReachabilityCheckerTest {
  private val dataplaneId = UUID.randomUUID()
  private val now = Instant.parse("2026-11-12T10:00:00Z")
  private val ownTestFilePrefix = "diagnostics/dataplanes/$dataplaneId/storage-check-"
  private val authorization =
    GcsDownscopedOAuthLogUploadAuthorization(
      accessToken = "token",
      expiresAt = OffsetDateTime.parse("2026-11-12T11:00:00Z"),
      bucketName = "airbyte-logs",
      objectKeyPrefix = "job-logging/diagnostics/dataplanes/$dataplaneId/",
    )

  private lateinit var workloadApiClient: WorkloadApiClient
  private lateinit var storageClientFactory: StorageClientFactory
  private lateinit var ownLogStorage: StorageClient
  private lateinit var airbyteLogStorage: AirbyteLogStorageWriter

  @BeforeEach
  fun setup() {
    workloadApiClient = mockk()
    storageClientFactory = mockk()
    ownLogStorage = mockk()
    airbyteLogStorage = mockk()
    every { workloadApiClient.dataplaneLogUploadAuthorization(dataplaneId.toString()) } returns null
    every { storageClientFactory.create(DocumentType.LOGS) } returns ownLogStorage
    every { ownLogStorage.write(any(), StorageReachabilityChecker.TEST_FILE_CONTENTS) } just runs
    every { ownLogStorage.delete(any()) } returns true
    every { airbyteLogStorage.write(authorization, any(), StorageReachabilityChecker.TEST_FILE_CONTENTS) } just runs
  }

  @Test
  fun `an upload to Airbyte's log storage passes`() {
    every { workloadApiClient.dataplaneLogUploadAuthorization(dataplaneId.toString()) } returns authorization
    val fileName = slot<String>()

    assertEquals(expected(DataplaneDiagnosticStatus.PASSED), checker().check(dataplaneId))
    verify { airbyteLogStorage.write(authorization, capture(fileName), StorageReachabilityChecker.TEST_FILE_CONTENTS) }
    assertTrue(fileName.captured.startsWith("storage-check-"), fileName.captured)
    verify(exactly = 0) { storageClientFactory.create(any()) }
  }

  @Test
  fun `each upload to Airbyte's log storage uses a new file name`() {
    every { workloadApiClient.dataplaneLogUploadAuthorization(dataplaneId.toString()) } returns authorization
    val fileNames = mutableListOf<String>()
    every { airbyteLogStorage.write(authorization, capture(fileNames), any()) } just runs
    val checker = checker()

    checker.check(dataplaneId)
    checker.check(dataplaneId)

    assertEquals(2, fileNames.toSet().size, "$fileNames")
  }

  @Test
  fun `a failed upload to Airbyte's log storage fails`() {
    every { workloadApiClient.dataplaneLogUploadAuthorization(dataplaneId.toString()) } returns authorization
    every { airbyteLogStorage.write(authorization, any(), any()) } throws RuntimeException("403 Forbidden")

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.FAILED,
        StorageReachabilityChecker.UPLOAD_FAILED,
        "The data plane could not upload to its sync log storage.",
      ),
      checker().check(dataplaneId),
    )
  }

  @Test
  fun `an upload to the data plane's own log storage passes and deletes the test file`() {
    val testFile = slot<String>()

    assertEquals(expected(DataplaneDiagnosticStatus.PASSED), checker().check(dataplaneId))
    verify { ownLogStorage.write(capture(testFile), StorageReachabilityChecker.TEST_FILE_CONTENTS) }
    assertTrue(testFile.captured.startsWith(ownTestFilePrefix), testFile.captured)
    verify { ownLogStorage.delete(testFile.captured) }
    verify(exactly = 0) { airbyteLogStorage.write(any(), any(), any()) }
  }

  @Test
  fun `each upload to the data plane's own log storage uses a new file name`() {
    val testFiles = mutableListOf<String>()
    every { ownLogStorage.write(capture(testFiles), any()) } just runs
    val checker = checker()

    checker.check(dataplaneId)
    checker.check(dataplaneId)

    assertEquals(2, testFiles.toSet().size, "$testFiles")
  }

  @Test
  fun `a test file that can't be deleted from the data plane's own log storage still passes`() {
    every { ownLogStorage.delete(any()) } throws RuntimeException("403 Forbidden")

    assertEquals(expected(DataplaneDiagnosticStatus.PASSED), checker().check(dataplaneId))
  }

  @Test
  fun `a failed upload to the data plane's own log storage fails`() {
    every { ownLogStorage.write(any(), any()) } throws RuntimeException("connection refused")

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.FAILED,
        StorageReachabilityChecker.UPLOAD_FAILED,
        "The data plane could not upload to its sync log storage.",
      ),
      checker().check(dataplaneId),
    )
    verify(exactly = 0) { ownLogStorage.delete(any()) }
  }

  @Test
  fun `a data plane's own log storage client that can't be built fails`() {
    every { storageClientFactory.create(DocumentType.LOGS) } throws IllegalStateException("missing credentials")

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.FAILED,
        StorageReachabilityChecker.UPLOAD_FAILED,
        "The data plane could not upload to its sync log storage.",
      ),
      checker().check(dataplaneId),
    )
  }

  @Test
  fun `the data plane's own log storage client is built once`() {
    val checker = checker()

    checker.check(dataplaneId)
    checker.check(dataplaneId)

    verify(exactly = 1) { storageClientFactory.create(DocumentType.LOGS) }
    verify(exactly = 2) { ownLogStorage.write(any(), StorageReachabilityChecker.TEST_FILE_CONTENTS) }
  }

  @Test
  fun `a blank log bucket is not configured`() {
    val checker = checker(AirbyteStorageConfig(bucket = AirbyteStorageConfig.AirbyteStorageBucketConfig(log = "")))

    assertEquals(expected(DataplaneDiagnosticStatus.NOT_CONFIGURED), checker.check(dataplaneId))
    verify(exactly = 0) { storageClientFactory.create(any()) }
  }

  @Test
  fun `an upload authorization request that fails is unable to complete`() {
    every { workloadApiClient.dataplaneLogUploadAuthorization(dataplaneId.toString()) } throws RuntimeException("503 Service Unavailable")

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        StorageReachabilityChecker.AUTHORIZATION_UNAVAILABLE,
        "Airbyte could not tell where this data plane's sync logs go.",
      ),
      checker().check(dataplaneId),
    )
    verify(exactly = 0) { storageClientFactory.create(any()) }
    verify(exactly = 0) { airbyteLogStorage.write(any(), any(), any()) }
  }

  @Test
  fun `a timed out check is unable to complete`() {
    assertEquals(
      expected(
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        StorageReachabilityChecker.TIMED_OUT,
        "The sync log storage did not respond in time.",
      ),
      checker().timedOut(dataplaneId),
    )
  }

  private fun checker(storageConfig: AirbyteStorageConfig = AirbyteStorageConfig()) =
    StorageReachabilityChecker(workloadApiClient, storageConfig, storageClientFactory, airbyteLogStorage, Clock.fixed(now, ZoneOffset.UTC))

  private fun expected(
    status: DataplaneDiagnosticStatus,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.STORAGE_REACHABILITY.value,
    targetId = dataplaneId,
    status = status,
    checkedAt = now.toEpochMilli(),
    errorCode = errorCode,
    errorMessage = errorMessage,
  )
}
