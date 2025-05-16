/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.status.ErrorStatus
import ch.qos.logback.core.status.Status
import io.airbyte.commons.storage.AirbyteCloudStorageBulkUploader
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.createFileId
import java.util.concurrent.TimeUnit

/**
 * A subclass of [AirbyteCloudStorageBulkUploader] so that we can override the upload() method
 * and use the bulkEncode method to handle converting the log events to strings (in the default
 * implementation we use [Jsons]).
 */
class AirbyteLogbackBulkUploader(
  baseStorageId: String,
  storageClient: StorageClient,
  period: Long = 60L,
  unit: TimeUnit = TimeUnit.SECONDS,
  private val encoder: AirbyteLogEventEncoder,
  private val addStatus: (Status) -> Unit,
) : AirbyteCloudStorageBulkUploader<ILoggingEvent>(
    baseStorageId,
    storageClient,
    period,
    unit,
  ) {
  override fun upload() {
    try {
      synchronized(uploadLock) {
        val events = mutableListOf<ILoggingEvent>()
        buffer.drainTo(events)

        if (events.isNotEmpty()) {
          val document = encoder.bulkEncode(loggingEvents = events)
          storageClient.write(id = currentStorageId, document = document)

          // Move to next file to avoid overwriting in log storage that doesn't support append mode
          this.currentStorageId = createFileId(baseId = baseStorageId)
        }
      }
    } catch (t: Throwable) {
      addStatus(ErrorStatus("Failed to upload logs to cloud storage location $currentStorageId.", this, t))
    }
  }
}
