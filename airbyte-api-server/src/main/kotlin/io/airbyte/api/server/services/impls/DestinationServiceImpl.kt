package io.airbyte.api.server.services.impls

import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.airbyte_api.model.generated.DestinationsResponse
import io.airbyte.api.server.services.DestinationService
import java.util.UUID

class DestinationServiceImpl : DestinationService {
  override fun createDestination(
    destinationCreateRequest: DestinationCreateRequest?,
    destinationDefinitionId: UUID?,
    userInfo: String,
  ): DestinationResponse {
    TODO("Not yet implemented")
  }

  override fun getDestination(destinationId: UUID?, userInfo: String): DestinationResponse {
    TODO("Not yet implemented")
  }

  override fun updateDestination(destinationId: UUID, destinationPutRequest: DestinationPutRequest, userInfo: String): DestinationResponse {
    TODO("Not yet implemented")
  }

  override fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    userInfo: String,
  ): DestinationResponse {
    TODO("Not yet implemented")
  }

  override fun deleteDestination(connectionId: UUID, userInfo: String) {
    TODO("Not yet implemented")
  }

  override fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    authorization: String,
    userInfo: String,
  ): DestinationsResponse? {
    TODO("Not yet implemented")
  }
}
