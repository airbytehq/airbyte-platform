/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.Job
import io.airbyte.data.repositories.entities.StreamStatusAuthorization
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class StreamStatusAuthorizationRepositoryTest : AbstractConfigRepositoryTest() {
  @Test
  fun `status ownership projection requires the saved workspace and excludes status payload metadata`() {
    val statuses = context.getBean(StreamStatusAuthorizationRepository::class.java)
    val id = UUID.randomUUID()
    val workspace = UUID.randomUUID()
    val connection = UUID.randomUUID()
    val job =
      jobsRepository.save(
        Job(
          71L,
          JobConfigType.sync,
          connection.toString(),
          Jsons.emptyObject(),
          JobStatus.running,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
          isScheduled = false,
        ),
      )
    jooqDslContext.execute(
      "INSERT INTO stream_statuses (id, workspace_id, connection_id, job_id, attempt_number, stream_name, job_type, run_state, transitioned_at, metadata) VALUES (?, ?, ?, ?, 0, 'orders', 'sync', 'running', CURRENT_TIMESTAMP, '{\"password\":\"unread-status-metadata\"}'::jsonb)",
      id,
      workspace,
      connection,
      job.id,
    )
    assertEquals(
      StreamStatusAuthorization(id, workspace, connection, job.id, 0, "sync"),
      statuses.findByIdAndWorkspaceId(id, workspace).orElseThrow(),
    )
    assertTrue(statuses.findByIdAndWorkspaceId(id, UUID.randomUUID()).isEmpty)
    assertTrue(statuses.findByIdAndWorkspaceId(UUID.randomUUID(), workspace).isEmpty)
  }
}
