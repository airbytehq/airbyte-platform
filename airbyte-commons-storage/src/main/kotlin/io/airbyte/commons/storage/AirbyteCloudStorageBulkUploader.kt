/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.json.Jsons
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * A generic storage uploader class that is intended to upload items to a
 * configured storage bucket, passed in through the StorageClient configuration.
 *
 * This class is open and intended to be subclassed whenever the default
 * method implementations are insufficient. Mostly, this will apply to the [upload]
 * method, which handles the logic for actually calling [StorageClient.write]. The
 * default implementations here should be sufficient for building a list and converting it
 * to JSON before uploading.
 *
 * To use this class, hook into the bean lifecycle methods (@PostConstruct/@PreDestroy,
 * or the Micronaut provided @EventListener) to call [start] and [stop], respectively. To
 * store a message, call [append]. Uploading will be done based on the period and unit
 * provided in the constructor. For an example, see [AuditLoggingInterceptor]
 */
open class AirbyteCloudStorageBulkUploader<T>(
  val baseStorageId: String,
  val storageClient: StorageClient,
  val period: Long = 60L,
  val unit: TimeUnit = TimeUnit.SECONDS,
) {
  /** @property buffer A threadsafe queue to store items of T */
  val buffer = LinkedBlockingQueue<T>()

  /** @property currentStorageId represents the id of the storage file */
  var currentStorageId: String = createFileId(baseStorageId)

  val uploadLock = Any()
  lateinit var uploadTask: ScheduledFuture<*>

  /**
   * Starts the upload task using the executor helper.
   */
  open fun start() {
    uploadTask = CloudStorageBulkUploaderExecutor.scheduleTask(this::upload, period, period, unit)
  }

  /**
   * Stops the upload task.
   */
  open fun stop() {
    // Perform one final flush before canceling the upload task
    upload()
    uploadTask.cancel(false)
  }

  /**
   * Add a message to the buffer.
   */
  open fun append(t: T) {
    buffer.add(t)
  }

  /**
   * Uploads all the items in the current buffer to cloud storage. This is done by first
   * combining all the buffered items and then converting to a JSON string.
   * The fileID is then updated to reflect a new timestamp of logs.
   */
  open fun upload() {
    synchronized(uploadLock) {
      val events = mutableListOf<T>()
      buffer.drainTo(events)

      if (events.isNotEmpty()) {
        try {
          val logs = Jsons.serialize(events)
          storageClient.write(currentStorageId, logs)
          this.currentStorageId = createFileId(baseStorageId)
        } catch (e: Exception) {
          // Log any failures to serialize or upload to cloud storage for debugging purposes
          e.printStackTrace()
        }
      }
    }
  }
}
