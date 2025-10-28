/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
class DataplaneGroupResolver(
  private val airbyteApi: AirbyteApiClient,
) {
  fun resolveForCheck(
    workspaceId: UUID,
    actorId: UUID?,
  ): String = getDataplaneGroupId(workspaceId, null)

  fun resolveForDiscover(
    workspaceId: UUID,
    actorId: UUID,
  ): String = getDataplaneGroupId(workspaceId, null)

  fun resolveForSync(
    workspaceId: UUID,
    connectionId: UUID,
  ): String = getDataplaneGroupId(workspaceId, connectionId)

  fun resolveForSpec(workspaceId: UUID): String = getDataplaneGroupId(workspaceId, null)

  private fun getDataplaneGroupId(
    workspaceId: UUID,
    connectionId: UUID?,
  ): String =
    try {
      airbyteApi.workspaceApi
        .getWorkspace(WorkspaceIdRequestBody(workspaceId))
        .dataplaneGroupId
        ?.toString()
        ?: ""
    } catch (e: ClientException) {
      when (e.statusCode) {
        404 -> {
          log.warn { "Workspace $workspaceId not found (404). Workspace may have been deleted." }
          throw WorkspaceNotFoundException.fromClientException(workspaceId, 404, e)
        }
        else -> {
          log.error(e) { "Failed to fetch workspace $workspaceId with status ${e.statusCode}" }
          throw e // Allow retry for transient errors
        }
      }
    } catch (e: io.airbyte.data.ConfigNotFoundException) {
      log.warn { "Workspace $workspaceId not found in database" }
      throw WorkspaceNotFoundException(workspaceId, "Workspace $workspaceId not found in database", e)
    }
}
