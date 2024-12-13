package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataplaneApi
import io.airbyte.api.model.generated.DataplaneGetIdRequestBody
import io.airbyte.api.model.generated.DataplaneRead
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.services.DataplaneService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/dataplanes")
@Secured(SecurityRule.IS_AUTHENTICATED)
class DataplaneController(
  private val dataplaneService: DataplaneService,
) : DataplaneApi {
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDataplaneId(dataplaneGetIdRequestBody: DataplaneGetIdRequestBody): DataplaneRead {
    val connectionId = dataplaneGetIdRequestBody.connectionId
    val actorType = dataplaneGetIdRequestBody.actorType
    val actorId = dataplaneGetIdRequestBody.actorId
    val workspaceId = dataplaneGetIdRequestBody.workspaceId
    val queueName = dataplaneService.getQueueName(connectionId, actorType, actorId, workspaceId, dataplaneGetIdRequestBody.workloadPriority)

    return DataplaneRead().id(queueName)
  }
}
