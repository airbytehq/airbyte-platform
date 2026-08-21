/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.read

import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.AuditLoggingEntitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.domain.models.OrganizationId
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

/**
 * Reads customer audit logs from object storage.
 *
 * Entries are stored one JSON array per file under
 * `<baseStorageId>/<organizationId>/<yyyy-MM-dd>/`. A query only ever lists the single
 * `<organizationId>/` prefix named by the query — tenant isolation is structural at this layer
 * and the caller (API controller) is responsible for deriving the organization from the
 * authenticated request context.
 *
 * Every read requires the organization to hold the audit-logging entitlement, checked before any
 * storage access. This mirrors the entitlement the interceptor enforces on the write path, so an
 * organization that was never entitled to have entries stored cannot list them either.
 *
 * Scan order is days descending from the end of the range, then files descending by name (the file
 * name starts with a `yyyyMMddHHmmss` timestamp, so lexicographic order is chronological), then
 * entries by timestamp descending. The returned page is sorted by timestamp descending. Because a
 * page may be assembled from a bounded scan, ordering is only guaranteed within a returned page.
 *
 * Read cost is bounded: at most [maxFilesPerRequest] files are scanned per call. When the scan
 * stops early (page full or file cap reached), the returned page carries an opaque resume token.
 * Files that cannot be read or parsed are skipped with a warning so that one corrupt blob does not
 * break a customer query.
 */
@Singleton
class AuditLogReadService {
  private val baseStorageId: String
  private val storageClient: StorageClient
  private val maxFilesPerRequest: Int
  private val entitlementService: EntitlementService

  @Inject
  constructor(
    storageConfiguration: AirbyteStorageConfig,
    storageClientFactory: StorageClientFactory,
    entitlementService: EntitlementService,
  ) : this(
    baseStorageId = storageConfiguration.bucket.auditLogging,
    storageClient = storageClientFactory.create(DocumentType.AUDIT_LOGS),
    maxFilesPerRequest = DEFAULT_MAX_FILES_PER_REQUEST,
    entitlementService = entitlementService,
  )

  @InternalForTesting
  internal constructor(
    baseStorageId: String,
    storageClient: StorageClient,
    maxFilesPerRequest: Int,
    entitlementService: EntitlementService,
  ) {
    this.baseStorageId = baseStorageId.trim('/')
    this.storageClient = storageClient
    this.maxFilesPerRequest = maxFilesPerRequest
    this.entitlementService = entitlementService
  }

  /**
   * Lists audit log entries for the query's organization. Throws
   * [io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem] when the organization
   * is not entitled to audit logging, and [IllegalArgumentException] for an invalid page token
   * (malformed, outside the query range, or referencing an unknown file).
   */
  fun listAuditLogs(query: AuditLogQuery): AuditLogPage {
    entitlementService.ensureEntitled(OrganizationId(query.organizationId), AuditLoggingEntitlement)

    val resume = query.pageToken?.let { AuditLogPageTokenCodec.decode(it) }
    val startDate = query.startTime.atZone(ZoneOffset.UTC).toLocalDate()
    val endDate = query.endTime.atZone(ZoneOffset.UTC).toLocalDate()

    if (resume != null && (resume.date.isBefore(startDate) || resume.date.isAfter(endDate))) {
      throw IllegalArgumentException("Page token is outside the query time range.")
    }

    val matches = mutableListOf<AuditLogEntry>()
    var filesScanned = 0
    var nextPageToken: String? = null

    scan@ for (date in daysDescending(startDate, resume?.date ?: endDate)) {
      val fileIds = listFileIdsForDate(query.organizationId, date)
      val filesToScan =
        if (resume != null && date == resume.date) {
          fileIds
            .dropWhile { it != resume.fileId }
            .ifEmpty { throw IllegalArgumentException("Page token references an unknown file.") }
        } else {
          fileIds
        }

      for (fileId in filesToScan) {
        if (filesScanned >= maxFilesPerRequest) {
          nextPageToken = AuditLogPageTokenCodec.encode(AuditLogPageToken(date, fileId, 0))
          break@scan
        }
        filesScanned++

        val entries = readEntries(fileId)
        val startIndex = if (resume != null && date == resume.date && fileId == resume.fileId) resume.index else 0

        for (index in startIndex until entries.size) {
          val entry = entries[index]
          if (matches(query, entry)) {
            matches.add(entry)
            if (matches.size >= query.pageSize) {
              nextPageToken = AuditLogPageTokenCodec.encode(AuditLogPageToken(date, fileId, index + 1))
              break@scan
            }
          }
        }
      }
    }

    return AuditLogPage(entries = matches.sortedByDescending { it.timestamp }, nextPageToken = nextPageToken)
  }

  private fun daysDescending(
    startDate: LocalDate,
    endDate: LocalDate,
  ): Sequence<LocalDate> = generateSequence(endDate) { it.minusDays(1).takeIf { next -> !next.isBefore(startDate) } }

  /**
   * Lists file ids for one organization+date partition, normalized across storage backends and
   * sorted descending. Cloud clients return full keys (including the `audit-logging/` document
   * type prefix) while [io.airbyte.commons.storage.LocalStorageClient] returns ids relative to
   * the prefix; the prefix is stripped so that [StorageClient.read] works for every backend.
   */
  private fun listFileIdsForDate(
    organizationId: UUID,
    date: LocalDate,
  ): List<String> =
    storageClient
      .list("$baseStorageId/$organizationId/$date")
      .map { it.removePrefix("$DOCUMENT_TYPE_PREFIX/") }
      .sortedDescending()

  private fun readEntries(fileId: String): List<AuditLogEntry> {
    val document =
      try {
        storageClient.read(fileId)
      } catch (e: Exception) {
        logger.warn(e) { "Failed to read audit log file $fileId; skipping it." }
        return emptyList()
      }

    if (document == null) {
      logger.warn { "Audit log file $fileId disappeared between list and read; skipping it." }
      return emptyList()
    }

    return try {
      Jsons
        .deserialize(document, object : TypeReference<List<AuditLogEntry>>() {})
        .sortedByDescending { it.timestamp }
    } catch (e: Exception) {
      logger.warn(e) { "Failed to parse audit log file $fileId; skipping it." }
      emptyList()
    }
  }

  private fun matches(
    query: AuditLogQuery,
    entry: AuditLogEntry,
  ): Boolean {
    if (entry.timestamp < query.startTime.toEpochMilli() || entry.timestamp > query.endTime.toEpochMilli()) return false
    if (query.workspaceId != null && entry.workspaceId != query.workspaceId) return false
    if (!query.actorId.isNullOrBlank() && !matchesActor(entry, query.actorId)) return false
    if (!query.operation.isNullOrBlank() && !entry.operation.contains(query.operation, ignoreCase = true)) return false
    if (query.success != null && entry.success != query.success) return false
    if (!query.searchText.isNullOrBlank() && !searchableText(entry).contains(query.searchText.lowercase())) return false
    return true
  }

  /**
   * Matches the actor filter against both the actor id and the actor email. Callers see the email
   * in the audit log UI and will type that, but the id is what the interceptor records, so
   * matching only one of the two makes the filter look broken. Substring and case-insensitive for
   * the same reason: an operator filtering by hand should not have to paste an exact UUID.
   */
  private fun matchesActor(
    entry: AuditLogEntry,
    actor: String,
  ): Boolean {
    val candidate = entry.actor ?: return false
    return candidate.actorId.contains(actor, ignoreCase = true) ||
      candidate.email?.contains(actor, ignoreCase = true) == true
  }

  /**
   * The fields a text search runs over. Request/response bodies are deliberately excluded: they
   * are large, and providers may include redacted-but-sensitive shapes we do not want to search.
   */
  private fun searchableText(entry: AuditLogEntry): String =
    listOfNotNull(entry.operation, entry.actor?.actorId, entry.actor?.email, entry.errorMessage)
      .joinToString("\n")
      .lowercase()

  companion object {
    private val logger = KotlinLogging.logger {}
    private val DOCUMENT_TYPE_PREFIX = DocumentType.AUDIT_LOGS.prefix.toString()

    /**
     * Default cap on the number of files scanned per request, bounding read latency.
     */
    const val DEFAULT_MAX_FILES_PER_REQUEST = 100
  }
}
