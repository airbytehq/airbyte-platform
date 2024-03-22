package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.WorkspaceRead
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class WorkspaceResonseMapperTest {
  @Test
  fun `from should convert a WorkspaceRead object from the config api to a WorkspaceResponse`() {
    // Given
    val workspaceRead =
      WorkspaceRead().apply {
        this.workspaceId = UUID.randomUUID()
        this.name = "workspaceName"
        this.email = "workspaceEmail@gmail.com"
      }

    // When
    val workspaceResponse = WorkspaceResponseMapper.from(workspaceRead)

    // Then
    assertEquals(workspaceRead.workspaceId, workspaceResponse.workspaceId)
    assertEquals(workspaceRead.name, workspaceResponse.name)
  }
}
