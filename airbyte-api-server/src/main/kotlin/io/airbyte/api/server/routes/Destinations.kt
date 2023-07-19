package io.airbyte.api.server.routes

import io.airbyte.airbyte_api.generated.DestinationsApi
import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.api.server.services.DestinationService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.core.Response

@Controller("/v1/destinations")
open class Destinations(private var destinationService: DestinationService) : DestinationsApi {
  override fun createDestination(destinationCreateRequest: DestinationCreateRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun deleteDestination(destinationId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun getDestination(destinationId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun listDestinations(workspaceIds: MutableList<UUID>?, includeDeleted: Boolean?, limit: Int?, offset: Int?): Response {
    TODO("Not yet implemented")
  }

  override fun patchDestination(destinationId: UUID?, destinationPatchRequest: DestinationPatchRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun putDestination(destinationId: UUID?, destinationPutRequest: DestinationPutRequest?): Response {
    TODO("Not yet implemented")
  }
}
