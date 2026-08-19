/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.AirbyteCloudStorageBulkUploader
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.createFileId
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Bulk uploader for audit log entries that partitions each flush by organization and date.
 *
 * Entries are written under `<organizationId>/<yyyy-MM-dd>/` below the base storage id
 * (itself beneath the `audit-logging/` document type prefix), one file per organization+date
 * partition. Entries without a resolved organization are considered internal-only and are written
 * under the `unknown` organization partition. The date partition is derived from each entry's
 * timestamp in UTC so that a flush spanning midnight still lands every entry in the partition
 * matching when it occurred.
 *
 * A failed write for one partition is logged and skipped without failing the remaining partitions
 * or the scheduled upload task.
 */
class AuditLogBulkUploader(
  baseStorageId: String,
  storageClient: StorageClient,
) : AirbyteCloudStorageBulkUploader<AuditLogEntry>(baseStorageId, storageClient) {
  /**
   * Drains the buffer and writes one file per organization+date partition. Does nothing when the
   * buffer is empty.
   */
  override fun upload() {
    val events = mutableListOf<AuditLogEntry>()
    synchronized(uploadLock) {
      buffer.drainTo(events)
    }

    if (events.isEmpty()) {
      return
    }

    events
      .groupBy { event ->
        Partition(
          organization = event.organizationId?.toString() ?: UNKNOWN_ORGANIZATION,
          date = DATE_FORMATTER.format(Instant.ofEpochMilli(event.timestamp)),
        )
      }.forEach { (partition, entries) ->
        uploadPartition(partition, entries)
      }
  }

  private fun uploadPartition(
    partition: Partition,
    entries: List<AuditLogEntry>,
  ) {
    try {
      val partitionStorageId = "${baseStorageId.trim('/')}/${partition.organization}/${partition.date}"
      storageClient.write(createFileId(partitionStorageId), Jsons.serialize(entries))
    } catch (e: Exception) {
      logger.error(e) { "Failed to upload ${entries.size} audit log entries for organization ${partition.organization}." }
    }
  }

  private data class Partition(
    val organization: String,
    val date: String,
  )

  companion object {
    private val logger = KotlinLogging.logger {}
    private val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC)

    /**
     * Organization partition segment for entries whose organization could not be resolved.
     */
    internal const val UNKNOWN_ORGANIZATION = "unknown"
  }
}
