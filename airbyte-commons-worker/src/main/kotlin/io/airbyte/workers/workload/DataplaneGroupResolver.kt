/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.Geography
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
  ): String = getGeography(workspaceId, null).value

  fun resolveForDiscover(
    organizationId: UUID,
    workspaceId: UUID,
    actorId: UUID,
  ): String = getGeography(workspaceId, null).value

  fun resolveForSync(
    organizationId: UUID,
    workspaceId: UUID,
    connectionId: UUID,
  ): String = getGeography(workspaceId, connectionId).value

  fun resolveForSpec(
    organizationId: UUID?,
    workspaceId: UUID?,
  ): String = getGeography(workspaceId, null).value

  // TODO Replace with the actual dataplaneGroup look up when defined
  private fun getGeography(
    workspaceId: UUID?,
    connectionId: UUID?,
  ): Geography {
    val geoFromConn =
      connectionId?.let {
        airbyteApi.connectionApi.getConnection(ConnectionIdRequestBody(it)).geography
      }
    return geoFromConn ?: workspaceId?.let { airbyteApi.workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId)).defaultGeography }
      ?: Geography.US
  }
}
