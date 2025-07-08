/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.repositories.entities.ConnectionTimelineEventMinimal
import io.airbyte.data.services.shared.ConnectionEvent
import io.micronaut.data.annotation.Expandable
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ConnectionTimelineEventRepository : PageableRepository<ConnectionTimelineEvent, UUID> {
  @Query(
    """
        SELECT * FROM connection_timeline_event
        WHERE connection_id = :connectionId
        AND ((:eventTypes) IS NULL OR event_type = ANY(CAST(ARRAY[:eventTypes] AS text[])) )
        AND (CAST(:createdAtStart AS timestamptz) IS NULL OR created_at >= CAST(:createdAtStart AS timestamptz))
        AND (CAST(:createdAtEnd AS timestamptz) IS NULL OR created_at <= CAST(:createdAtEnd AS timestamptz))
        ORDER BY created_at DESC
        LIMIT :pageSize
        OFFSET :rowOffset
    """,
  )
  fun findByConnectionIdWithFilters(
    connectionId: UUID,
    @Expandable eventTypes: List<ConnectionEvent.Type>?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    pageSize: Int,
    rowOffset: Int,
  ): List<ConnectionTimelineEvent>

  @Query(
    """
      SELECT cte.id, cte.connection_id, cte.event_type, cte.created_at, c.name AS connection_name
      FROM connection_timeline_event cte
      LEFT JOIN connection c ON c.id = cte.connection_id
      WHERE cte.connection_id IN (:connectionIds)
      AND cte.event_type IN (:eventTypes)
      AND (cte.created_at >= :createdAtStart)
      AND (cte.created_at <= :createdAtEnd)
    """,
  )
  fun findByConnectionIdsMinimal(
    connectionIds: List<UUID>,
    eventTypes: List<ConnectionEvent.Type>,
    createdAtStart: OffsetDateTime,
    createdAtEnd: OffsetDateTime,
  ): List<ConnectionTimelineEventMinimal>

  @Query(
    """
        SELECT user_id FROM connection_timeline_event
        WHERE connection_id = :connectionId
        AND (summary->>'jobId')::bigint = :jobId
        AND (:eventType IS NULL OR event_type = :eventType)
        AND (CAST(:createdAtStart AS timestamptz) IS NULL OR created_at >= CAST(:createdAtStart AS timestamptz))
        ORDER BY created_at
    """,
  )
  fun findAssociatedUserForAJob(
    connectionId: UUID,
    jobId: Long,
    eventType: ConnectionEvent.Type?,
    createdAtStart: OffsetDateTime?,
  ): List<UUID?>
}
