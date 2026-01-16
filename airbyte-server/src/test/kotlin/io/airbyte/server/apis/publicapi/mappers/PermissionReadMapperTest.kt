/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class PermissionReadMapperTest {
  @Test
  fun `should convert a PermissionRead object from the config api to a PermissionResponse`() {
    // Given
    val permissionRead =
      PermissionRead().apply {
        this.permissionId = UUID.randomUUID()
        this.permissionType = PermissionType.WORKSPACE_EDITOR
        this.userId = UUID.randomUUID()
        this.workspaceId = UUID.randomUUID()
        this.organizationId = null
      }

    // When
    val permissionResponse = PermissionReadMapper.from(permissionRead)

    // Then
    assertEquals(permissionRead.permissionId, permissionResponse.permissionId)
    assertEquals(permissionRead.permissionType.toString(), permissionResponse.permissionType.toString())
    assertEquals(permissionRead.userId, permissionResponse.userId)
    assertEquals(permissionRead.workspaceId, permissionResponse.workspaceId)
    assertEquals(permissionRead.organizationId, permissionResponse.organizationId)
  }
}
