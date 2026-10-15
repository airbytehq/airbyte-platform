/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.StreamStatusesApi
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.Pagination
import io.airbyte.api.server.generated.models.StreamStatusCreateRequestBody
import io.airbyte.api.server.generated.models.StreamStatusIncompleteRunCause
import io.airbyte.api.server.generated.models.StreamStatusListRequestBody
import io.airbyte.api.server.generated.models.StreamStatusRead
import io.airbyte.api.server.generated.models.StreamStatusReadList
import io.airbyte.api.server.generated.models.StreamStatusRunState
import io.airbyte.api.server.generated.models.StreamStatusUpdateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.handlers.StreamStatusesHandler
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import java.io.Serial

@Controller
class StreamStatusesApiController(
  private val handler: StreamStatusesHandler,
) : StreamStatusesApi {
  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createStreamStatus(
    @Body req: StreamStatusCreateRequestBody?,
  ): StreamStatusRead {
    Validations.validate(req!!.runState, req.incompleteRunCause)

    return handler.createStreamStatus(req)
  }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateStreamStatus(
    @Body req: StreamStatusUpdateRequestBody?,
  ): StreamStatusRead {
    Validations.validate(req!!.runState, req.incompleteRunCause)

    return handler.updateStreamStatus(req)
  }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getStreamStatuses(
    @Body req: StreamStatusListRequestBody?,
  ): StreamStatusReadList {
    Validations.validate(req!!.pagination)

    return handler.listStreamStatus(req)
  }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getStreamStatusesByRunState(
    @Body req: ConnectionIdRequestBody?,
  ): StreamStatusReadList = handler.listStreamStatusPerRunState(req!!)

  /**
   * Stateless request body validations.
   */
  internal object Validations {
    val PAGE_MIN: Int = 1

    val OFFSET_MIN: Int = 0

    fun validate(
      runState: StreamStatusRunState,
      incompleteRunCause: StreamStatusIncompleteRunCause?,
    ) {
      if (runState != StreamStatusRunState.INCOMPLETE && incompleteRunCause != null) {
        throw object : BadRequestException("Incomplete run cause may only be set for runs that stopped in an incomplete state.") {
          @Serial
          private val serialVersionUID = 2755161328698829068L
        }
      }
      if (runState == StreamStatusRunState.INCOMPLETE && incompleteRunCause == null) {
        throw object : BadRequestException("Incomplete run cause must be set for runs that stopped in an incomplete state.") {
          @Serial
          private val serialVersionUID = -7206700955952601425L
        }
      }
    }

    fun validate(pagination: Pagination?) {
      if (pagination == null) {
        throw BadRequestException("Pagination params must be provided.")
      }
      val pageSize = pagination.pageSize ?: throw BadRequestException("Page size must be provided.")
      val rowOffset = pagination.rowOffset ?: throw BadRequestException("Row offset must be provided.")
      if (pageSize < PAGE_MIN) {
        throw BadRequestException("Page size must be at least 1.")
      }
      if (rowOffset < OFFSET_MIN) {
        throw BadRequestException("Row offset cannot be less than 0.")
      }
      if (rowOffset % pageSize > 0) {
        throw BadRequestException("Row offset must be evenly divisible by page size.")
      }
    }
  }
}
