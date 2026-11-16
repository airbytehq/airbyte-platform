/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.workload.api.client.WorkloadApiClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Uploads a test file to where this data plane's sync logs go: Airbyte's log bucket when Airbyte issues an upload
 * authorization for it, otherwise the log bucket in its own storage settings.
 */
@Singleton
class StorageReachabilityChecker(
  private val workloadApiClient: WorkloadApiClient,
  private val storageConfig: AirbyteStorageConfig,
  private val storageClientFactory: StorageClientFactory,
  private val airbyteLogStorage: AirbyteLogStorageWriter,
  private val clock: Clock,
) {
  // Built on first use and kept, because building a client also creates its bucket when it's missing.
  private val ownLogStorage by lazy { storageClientFactory.create(DocumentType.LOGS) }

  // Every failure is reported as the result instead of thrown.
  fun check(dataplaneId: UUID): DataplaneDiagnosticResult {
    val authorization =
      try {
        workloadApiClient.dataplaneLogUploadAuthorization(dataplaneId.toString())
      } catch (e: Exception) {
        logger.warn(e) { "Could not get the log upload authorization for data plane $dataplaneId" }
        return result(
          dataplaneId,
          DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
          AUTHORIZATION_UNAVAILABLE,
          "Airbyte could not tell where this data plane's sync logs go.",
        )
      }

    // Airbyte's upload authorization can create files but not replace them.
    val testFileName = "$TEST_FILE_NAME-${UUID.randomUUID()}"
    return when {
      authorization != null ->
        upload(dataplaneId) { airbyteLogStorage.write(authorization, testFileName, TEST_FILE_CONTENTS) }
      storageConfig.bucket.log.isBlank() -> result(dataplaneId, DataplaneDiagnosticStatus.NOT_CONFIGURED)
      else -> {
        val testFile = ownTestFile(dataplaneId, testFileName)
        upload(dataplaneId) { ownLogStorage.write(testFile, TEST_FILE_CONTENTS) }.also {
          if (it.status == DataplaneDiagnosticStatus.PASSED) deleteOwnTestFile(testFile)
        }
      }
    }
  }

  // The check is about the upload, so a failed delete doesn't fail it.
  private fun deleteOwnTestFile(testFile: String) {
    try {
      ownLogStorage.delete(testFile)
    } catch (e: Exception) {
      logger.warn(e) { "Could not delete the test file $testFile from the sync log storage" }
    }
  }

  fun timedOut(dataplaneId: UUID): DataplaneDiagnosticResult =
    result(dataplaneId, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, TIMED_OUT, "The sync log storage did not respond in time.")

  private fun upload(
    dataplaneId: UUID,
    write: () -> Unit,
  ): DataplaneDiagnosticResult =
    try {
      write()
      result(dataplaneId, DataplaneDiagnosticStatus.PASSED)
    } catch (e: Exception) {
      logger.warn(e) { "Could not upload the test file to the sync log storage" }
      result(dataplaneId, DataplaneDiagnosticStatus.FAILED, UPLOAD_FAILED, "The data plane could not upload to its sync log storage.")
    }

  private fun result(
    dataplaneId: UUID,
    status: DataplaneDiagnosticStatus,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.STORAGE_REACHABILITY.value,
    targetId = dataplaneId,
    status = status,
    checkedAt = clock.millis(),
    errorCode = errorCode,
    errorMessage = errorMessage,
  )

  companion object {
    const val AUTHORIZATION_UNAVAILABLE = "AUTHORIZATION_UNAVAILABLE"
    const val UPLOAD_FAILED = "UPLOAD_FAILED"
    const val TIMED_OUT = "TIMED_OUT"

    private const val TEST_FILE_NAME = "storage-check"
    const val TEST_FILE_CONTENTS = "Data plane sync log storage is reachable"

    // The same folder Airbyte's upload authorization is limited to.
    private fun ownTestFile(
      dataplaneId: UUID,
      fileName: String,
    ) = "diagnostics/dataplanes/$dataplaneId/$fileName"
  }
}
