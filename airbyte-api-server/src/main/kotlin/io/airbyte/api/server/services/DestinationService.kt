package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.airbyte_api.model.generated.DestinationsResponse
import java.util.UUID
import javax.validation.constraints.NotBlank

interface DestinationService {
  fun createDestination(
    destinationCreateRequest: @NotBlank DestinationCreateRequest?,
    destinationDefinitionId: @NotBlank UUID?,
    userInfo: String,
  ): DestinationResponse

  fun getDestination(destinationId: @NotBlank UUID?, userInfo: String): DestinationResponse

  fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    userInfo: String,
  ): DestinationResponse

  fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    userInfo: String,
  ): DestinationResponse

  fun deleteDestination(connectionId: @NotBlank UUID, userInfo: String)

  fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    authorization: String,
    userInfo: String,
  ): DestinationsResponse?
}
