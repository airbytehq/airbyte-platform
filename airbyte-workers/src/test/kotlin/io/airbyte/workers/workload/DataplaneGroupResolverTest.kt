/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.config.ConfigNotFoundType
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

class DataplaneGroupResolverTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var workspaceApi: WorkspaceApi
  private lateinit var resolver: DataplaneGroupResolver

  @BeforeEach
  fun setup() {
    airbyteApiClient = mockk()
    workspaceApi = mockk()
    every { airbyteApiClient.workspaceApi } returns workspaceApi
    resolver = DataplaneGroupResolver(airbyteApiClient)
  }

  @Test
  fun `resolveForDiscover returns dataplane group ID`() {
    val workspaceId = UUID.randomUUID()
    val actorId = UUID.randomUUID()
    val expectedDataplaneGroupId = UUID.randomUUID()

    val workspace =
      mockk<WorkspaceRead> {
        every { dataplaneGroupId } returns expectedDataplaneGroupId
      }

    every { workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId)) } returns workspace

    val result = resolver.resolveForDiscover(workspaceId, actorId)

    assertEquals(expectedDataplaneGroupId.toString(), result)
    verify(exactly = 1) { workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId)) }
  }

  @Test
  fun `resolveForDiscover throws WorkspaceNotFoundException on 404`() {
    val workspaceId = UUID.randomUUID()
    val actorId = UUID.randomUUID()

    every { workspaceApi.getWorkspace(any()) } throws ClientException("Not Found", 404)

    val exception =
      assertThrows<WorkspaceNotFoundException> {
        resolver.resolveForDiscover(workspaceId, actorId)
      }

    assertEquals(workspaceId, exception.workspaceId)
    assertTrue(exception.message!!.contains("not found"))
    assertTrue(exception.message!!.contains("deleted"))
  }

  @Test
  fun `resolveForDiscover propagates non-404 ClientExceptions`() {
    val workspaceId = UUID.randomUUID()
    val actorId = UUID.randomUUID()

    every { workspaceApi.getWorkspace(any()) } throws ClientException("Server Error", 500)

    // Should propagate, not convert to WorkspaceNotFoundException
    assertThrows<ClientException> {
      resolver.resolveForDiscover(workspaceId, actorId)
    }
  }

  @Test
  fun `resolveForDiscover throws WorkspaceNotFoundException on ConfigNotFoundException`() {
    val workspaceId = UUID.randomUUID()
    val actorId = UUID.randomUUID()

    every { workspaceApi.getWorkspace(any()) } throws
      io.airbyte.data.ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, workspaceId)

    val exception =
      assertThrows<WorkspaceNotFoundException> {
        resolver.resolveForDiscover(workspaceId, actorId)
      }

    assertEquals(workspaceId, exception.workspaceId)
    assertTrue(exception.message!!.contains("database"))
  }

  @Test
  fun `resolveForDiscover returns empty string when dataplaneGroupId is null`() {
    val workspaceId = UUID.randomUUID()
    val actorId = UUID.randomUUID()

    val workspace =
      mockk<WorkspaceRead> {
        every { dataplaneGroupId } returns null
      }

    every { workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId)) } returns workspace

    val result = resolver.resolveForDiscover(workspaceId, actorId)

    assertEquals("", result)
  }
}
