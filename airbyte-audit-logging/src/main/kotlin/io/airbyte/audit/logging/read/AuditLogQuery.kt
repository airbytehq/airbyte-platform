/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.read

import io.airbyte.audit.logging.model.AuditLogEntry
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.Base64
import java.util.UUID

/**
 * A single customer audit-log query.
 *
 * The time range is inclusive on both ends. Defaults: [endTime] = now, [startTime] = now - 30 days.
 * Ranges are capped at [MAX_RANGE_DAYS] days and [pageSize] at [MAX_PAGE_SIZE] so that a single
 * request has bounded cost.
 *
 * Filters are convenience filters for organization admins; they are not an authorization boundary.
 * [pageToken] must be one returned by a previous call with otherwise-identical query parameters.
 */
data class AuditLogQuery(
  val organizationId: UUID,
  val startTime: Instant,
  val endTime: Instant,
  val workspaceId: UUID? = null,
  val actorId: String? = null,
  val operation: String? = null,
  val success: Boolean? = null,
  val searchText: String? = null,
  val pageSize: Int = DEFAULT_PAGE_SIZE,
  val pageToken: String? = null,
) {
  init {
    require(!startTime.isAfter(endTime)) { "startTime must not be after endTime." }
    require(ChronoUnit.DAYS.between(startTime, endTime) <= MAX_RANGE_DAYS) {
      "Time range must not exceed $MAX_RANGE_DAYS days."
    }
    require(pageSize in 1..MAX_PAGE_SIZE) { "pageSize must be between 1 and $MAX_PAGE_SIZE." }
  }

  companion object {
    const val DEFAULT_PAGE_SIZE = 50
    const val MAX_PAGE_SIZE = 200
    const val MAX_RANGE_DAYS = 90L
    const val DEFAULT_LOOKBACK_DAYS = 30L

    /**
     * Builds a query, applying the default time range ([DEFAULT_LOOKBACK_DAYS] days ending now)
     * for any bound that is not provided.
     */
    fun of(
      organizationId: UUID,
      startTime: Instant? = null,
      endTime: Instant? = null,
      workspaceId: UUID? = null,
      actorId: String? = null,
      operation: String? = null,
      success: Boolean? = null,
      searchText: String? = null,
      pageSize: Int? = null,
      pageToken: String? = null,
    ): AuditLogQuery {
      val resolvedEndTime = endTime ?: Instant.now()
      val resolvedStartTime = startTime ?: resolvedEndTime.minus(DEFAULT_LOOKBACK_DAYS, ChronoUnit.DAYS)
      return AuditLogQuery(
        organizationId = organizationId,
        startTime = resolvedStartTime,
        endTime = resolvedEndTime,
        workspaceId = workspaceId,
        actorId = actorId,
        operation = operation,
        success = success,
        searchText = searchText,
        pageSize = pageSize ?: DEFAULT_PAGE_SIZE,
        pageToken = pageToken,
      )
    }
  }
}

/**
 * One page of audit log entries, sorted by timestamp descending. When [nextPageToken] is non-null,
 * more entries may be retrieved by repeating the query with that token.
 */
data class AuditLogPage(
  val entries: List<AuditLogEntry>,
  val nextPageToken: String? = null,
)

/**
 * Position marker for resuming a scan: the next entry to consider is at [index] within [fileId]
 * under the [date] partition (all in the query's organization prefix).
 */
internal data class AuditLogPageToken(
  val date: LocalDate,
  val fileId: String,
  val index: Int,
)

/**
 * Opaque codec for [AuditLogPageToken]: base64url of `date|fileId|index`. File ids never contain
 * the separator (they are built from a timestamp, hostname, and UUID).
 */
internal object AuditLogPageTokenCodec {
  private const val SEPARATOR = "|"
  private const val PARTS = 3

  fun encode(token: AuditLogPageToken): String =
    Base64
      .getUrlEncoder()
      .withoutPadding()
      .encodeToString("${token.date}$SEPARATOR${token.fileId}$SEPARATOR${token.index}".toByteArray(Charsets.UTF_8))

  fun decode(value: String): AuditLogPageToken {
    val decoded =
      try {
        String(Base64.getUrlDecoder().decode(value), Charsets.UTF_8)
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("Invalid audit log page token.", e)
      }

    val parts = decoded.split(SEPARATOR, limit = PARTS)
    if (parts.size != PARTS || parts[1].isBlank()) {
      throw IllegalArgumentException("Invalid audit log page token.")
    }

    return try {
      AuditLogPageToken(
        date = LocalDate.parse(parts[0]),
        fileId = parts[1],
        index = parts[2].toInt().also { require(it >= 0) },
      )
    } catch (e: Exception) {
      throw IllegalArgumentException("Invalid audit log page token.", e)
    }
  }
}
