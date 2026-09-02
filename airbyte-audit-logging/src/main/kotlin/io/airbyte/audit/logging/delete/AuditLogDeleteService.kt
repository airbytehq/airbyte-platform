/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.delete

import io.airbyte.commons.server.handlers.dsr.DsrAuditLogDeletion
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.STRUCTURED_LOG_FILE_EXTENSION
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Deletes customer audit logs from object storage, keyed on organization.
 *
 * Entries are written by [io.airbyte.audit.logging.AuditLogBulkUploader] one JSON file per flush
 * under `<baseStorageId>/<organizationId>/<yyyy-MM-dd>/` below the `audit-logging/` document type
 * prefix. This service walks that layout and deletes every file below the organization's prefix.
 *
 * Storage backends enumerate that prefix differently, so the walk handles each shape:
 * GCS/S3 return every file key recursively in one listing, while Azure hierarchy listings return
 * one level at a time and represent date partitions as prefix entries ending in `/`. Entries that
 * are not files are recursed into.
 *
 * No entitlement check is performed: this runs on the GDPR / DSR deletion path after the
 * organization's entitlement row is gone, and an unentitled organization has no stored entries to
 * delete anyway.
 *
 * Deletion is organization-scoped, not actor-scoped: the DSR runbook only invokes it for the
 * organizations the data subject owned, so audit log entries recorded under other tenants where
 * the data subject was merely a member are not removed by this service.
 */
@Singleton
class AuditLogDeleteService(
  private val baseStorageId: String,
  private val storageClient: StorageClient,
) : DsrAuditLogDeletion {
  @Inject
  constructor(
    storageConfiguration: AirbyteStorageConfig,
    storageClientFactory: StorageClientFactory,
  ) : this(
    baseStorageId = storageConfiguration.bucket.auditLogging,
    storageClient = storageClientFactory.create(DocumentType.AUDIT_LOGS),
  )

  override fun deleteAuditLogsByOrganizationId(organizationId: UUID): Int {
    if (baseStorageId.isBlank()) {
      logger.info { "Audit logging storage bucket is not configured; nothing was stored, so nothing to delete for organization $organizationId." }
      return 0
    }
    val deletedCount = deletePartition("$baseStorageId/$organizationId")
    logger.info { "Deleted $deletedCount audit log file(s) for organization $organizationId." }
    return deletedCount
  }

  /**
   * Deletes every audit log file below [partitionId]. Cloud clients return full storage keys
   * (including the `audit-logging/` document type prefix) while
   * [io.airbyte.commons.storage.LocalStorageClient] returns ids relative to the prefix; the prefix
   * is stripped so that [StorageClient.delete] works for every backend. Non-file entries are date
   * partition prefix entries (Azure) and are recursed into.
   */
  private fun deletePartition(partitionId: String): Int =
    storageClient
      .list(partitionId)
      .map { fileId -> fileId.removePrefix("$DOCUMENT_TYPE_PREFIX/") }
      .sumOf { fileId ->
        if (fileId.endsWith(STRUCTURED_LOG_FILE_EXTENSION)) {
          if (storageClient.delete(fileId)) 1 else 0
        } else {
          deletePartition(fileId.trimEnd('/'))
        }
      }

  companion object {
    private val logger: KLogger = KotlinLogging.logger {}
    private val DOCUMENT_TYPE_PREFIX = DocumentType.AUDIT_LOGS.prefix.toString()
  }
}
