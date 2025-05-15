/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicDataplanesApi
import io.airbyte.publicApi.server.generated.models.DataplaneCreateRequest
import io.airbyte.publicApi.server.generated.models.DataplanePatchRequest
import io.airbyte.server.apis.publicapi.constants.DATAPLANES_PATH
import io.airbyte.server.apis.publicapi.services.DataplaneService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(DATAPLANES_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataplaneController(
  private val dataplaneService: DataplaneService,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicDataplanesApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListDataplanes(): Response {
    val userId = currentUserService.currentUser.userId
    apiAuthorizationHelper.isUserInstanceAdminOrThrow(userId)
    return dataplaneService.controllerListDataplanes()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateDataplane(dataplaneCreateRequest: DataplaneCreateRequest): Response {
    val userId = currentUserService.currentUser.userId
    apiAuthorizationHelper.isUserInstanceAdminOrThrow(userId)
    return dataplaneService.controllerCreateDataplane(dataplaneCreateRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$DATAPLANES_PATH/{dataplaneId}")
  override fun publicGetDataplane(dataplaneId: UUID): Response {
    val userId = currentUserService.currentUser.userId
    apiAuthorizationHelper.isUserInstanceAdminOrThrow(userId)
    return dataplaneService.controllerGetDataplane(dataplaneId)
  }

  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$DATAPLANES_PATH/{dataplaneId}")
  override fun publicUpdateDataplane(
    dataplaneId: UUID,
    dataplanePatchRequest: DataplanePatchRequest,
  ): Response {
    val userId = currentUserService.currentUser.userId
    apiAuthorizationHelper.isUserInstanceAdminOrThrow(userId)
    return dataplaneService.controllerUpdateDataplane(dataplaneId, dataplanePatchRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$DATAPLANES_PATH/{dataplaneId}")
  override fun publicDeleteDataplane(dataplaneId: UUID): Response {
    val userId = currentUserService.currentUser.userId
    apiAuthorizationHelper.isUserInstanceAdminOrThrow(userId)
    return dataplaneService.controllerDeleteDataplane(dataplaneId)
  }
}
