package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataplaneApi
import io.airbyte.api.model.generated.DataplaneGetIdRequestBody
import io.airbyte.api.model.generated.DataplaneRead
import io.airbyte.server.services.DataplaneService
import io.micronaut.http.annotation.Controller

@Controller("/api/v1/dataplanes")
class DataplaneController(
  private val dataplaneService: DataplaneService,
) : DataplaneApi {
  override fun getDataplaneId(dataplaneGetIdRequestBody: DataplaneGetIdRequestBody): DataplaneRead {
    val connectionId = dataplaneGetIdRequestBody.connectionId
    val actorType = dataplaneGetIdRequestBody.actorType
    val actorId = dataplaneGetIdRequestBody.actorId
    val workspaceId = dataplaneGetIdRequestBody.workspaceId
    val queueName = dataplaneService.getQueueName(connectionId, actorType, actorId, workspaceId, dataplaneGetIdRequestBody.workloadPriority)

    return DataplaneRead().id(queueName)
  }
}
