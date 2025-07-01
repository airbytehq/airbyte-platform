/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.RefreshStream
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface StreamRefreshesRepository : PageableRepository<StreamRefresh, UUID> {
  fun findByConnectionId(connectionId: UUID): List<StreamRefresh>

  fun deleteByConnectionId(connectionId: UUID)

  @Query(
    value = """
            DELETE FROM stream_refreshes 
            WHERE connection_id = :connectionId 
            AND stream_name = :streamName
            AND ((:streamNamespace) IS NULL OR stream_namespace = :streamNamespace)
        """,
  )
  fun deleteByConnectionIdAndStreamNameAndStreamNamespace(
    connectionId: UUID,
    streamName: String,
    streamNamespace: String?,
  )

  fun existsByConnectionId(connectionId: UUID): Boolean
}

fun StreamRefreshesRepository.saveStreamsToRefresh(
  connectionId: UUID,
  streamDescriptors: List<StreamDescriptor>,
  refreshType: RefreshStream.RefreshType = RefreshStream.RefreshType.MERGE,
) {
  // don't create refreshes for streams already pending refresh
  val exists =
    findByConnectionId(connectionId)
      .map { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) }
      .toSet()

  val refreshes =
    streamDescriptors
      .filterNot { exists.contains(it) }
      .map { s ->
        StreamRefresh(
          connectionId = connectionId,
          streamName = s.name,
          streamNamespace = s.namespace,
          refreshType = refreshType.toDBO(),
        )
      }
  saveAll(refreshes)
}

fun RefreshStream.RefreshType.toDBO(): RefreshType =
  when (this) {
    RefreshStream.RefreshType.MERGE -> RefreshType.MERGE
    RefreshStream.RefreshType.TRUNCATE -> RefreshType.TRUNCATE
  }
