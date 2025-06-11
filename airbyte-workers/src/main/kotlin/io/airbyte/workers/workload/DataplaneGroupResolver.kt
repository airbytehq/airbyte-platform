/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class DataplaneGroupResolver(
  private val airbyteApi: AirbyteApiClient,
) {
  fun resolveForCheck(
    organizationId: UUID,
    workspaceId: UUID,
    actorId: UUID?,
  ): String = getDataplaneGroupId(workspaceId, null)

  fun resolveForDiscover(
    organizationId: UUID,
    workspaceId: UUID,
    actorId: UUID,
  ): String = getDataplaneGroupId(workspaceId, null)

  fun resolveForSync(
    organizationId: UUID,
    workspaceId: UUID,
    connectionId: UUID,
  ): String = getDataplaneGroupId(workspaceId, connectionId)

  fun resolveForSpec(
    organizationId: UUID?,
    workspaceId: UUID,
  ): String = getDataplaneGroupId(workspaceId, null)

  private fun getDataplaneGroupId(
    workspaceId: UUID,
    connectionId: UUID?,
  ): String =
    airbyteApi.workspaceApi
      .getWorkspace(WorkspaceIdRequestBody(workspaceId))
      .dataplaneGroupId
      ?.toString()
      ?: ""
}
