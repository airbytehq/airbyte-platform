/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.repositories.StreamStatusAuthorizationRepository
import io.airbyte.data.repositories.entities.StreamStatusAuthorization
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class StreamStatusAuthorizationDataServiceTest {
  @Test
  fun `status metadata preserves every parent and rejects missing wrong identity foreign scope and dependency failures`() {
    val id = UUID.randomUUID()
    val workspace = UUID.randomUUID()
    val saved = StreamStatusAuthorization(id, workspace, UUID.randomUUID(), 47L, 0, "sync")
    val rows = mockk<StreamStatusAuthorizationRepository>()
    val service = StreamStatusAuthorizationDataService(rows)
    every { rows.findByIdAndWorkspaceId(id, workspace) } returns Optional.of(saved)
    assertEquals(saved, service.status(id, workspace))
    every { rows.findByIdAndWorkspaceId(id, workspace) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.status(id, workspace) }
    every { rows.findByIdAndWorkspaceId(id, workspace) } returns Optional.of(saved.copy(id = UUID.randomUUID()))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.status(id, workspace) }
    every { rows.findByIdAndWorkspaceId(id, workspace) } returns Optional.of(saved.copy(workspaceId = UUID.randomUUID()))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.status(id, workspace) }
    val failure = IOException("saved status ownership unavailable")
    every { rows.findByIdAndWorkspaceId(id, workspace) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.status(id, workspace) })
  }
}
