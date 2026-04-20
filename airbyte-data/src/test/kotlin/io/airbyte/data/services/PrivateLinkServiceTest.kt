/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.PrivateLinkRepository
import io.airbyte.domain.models.PrivateLinkStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.data.repositories.entities.PrivateLink as EntityPrivateLink
import io.airbyte.db.instance.configs.jooq.generated.enums.PrivateLinkStatus as EntityPrivateLinkStatus

class PrivateLinkServiceTest {
  private val repository: PrivateLinkRepository = mockk()
  private val service = PrivateLinkService(repository)

  private val workspaceId = UUID.randomUUID()
  private val dataplaneGroupId = UUID.randomUUID()

  private fun entity(
    id: UUID = UUID.randomUUID(),
    name: String,
    status: EntityPrivateLinkStatus,
  ) = EntityPrivateLink(
    id = id,
    workspaceId = workspaceId,
    dataplaneGroupId = dataplaneGroupId,
    name = name,
    status = status,
    serviceRegion = "us-east-1",
    serviceName = "com.amazonaws.vpce.us-east-1.vpce-svc-test",
  )

  @Test
  fun `listByWorkspaceId excludes DELETED rows`() {
    every { repository.findByWorkspaceId(workspaceId) } returns
      listOf(
        entity(name = "live", status = EntityPrivateLinkStatus.available),
        entity(name = "in-progress", status = EntityPrivateLinkStatus.deleting),
        entity(name = "tombstoned", status = EntityPrivateLinkStatus.deleted),
      )

    val result = service.listByWorkspaceId(workspaceId)

    assertEquals(2, result.size)
    assertEquals(setOf("live", "in-progress"), result.map { it.name }.toSet())
    assertEquals(false, result.any { it.status == PrivateLinkStatus.DELETED })
  }

  @Test
  fun `listByWorkspaceId returns empty when only DELETED rows exist`() {
    every { repository.findByWorkspaceId(workspaceId) } returns
      listOf(
        entity(name = "tombstoned-1", status = EntityPrivateLinkStatus.deleted),
        entity(name = "tombstoned-2", status = EntityPrivateLinkStatus.deleted),
      )

    val result = service.listByWorkspaceId(workspaceId)

    assertEquals(0, result.size)
  }
}
