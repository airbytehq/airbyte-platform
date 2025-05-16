/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataplaneApi
import io.airbyte.api.model.generated.AccessToken
import io.airbyte.api.model.generated.DataplaneCreateRequestBody
import io.airbyte.api.model.generated.DataplaneCreateResponse
import io.airbyte.api.model.generated.DataplaneDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.model.generated.DataplaneHeartbeatResponse
import io.airbyte.api.model.generated.DataplaneInitRequestBody
import io.airbyte.api.model.generated.DataplaneInitResponse
import io.airbyte.api.model.generated.DataplaneListRequestBody
import io.airbyte.api.model.generated.DataplaneListResponse
import io.airbyte.api.model.generated.DataplaneRead
import io.airbyte.api.model.generated.DataplaneTokenRequestBody
import io.airbyte.api.model.generated.DataplaneUpdateRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.Dataplane
import io.airbyte.data.services.DataplaneCredentialsService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.server.services.DataplaneService
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.stream.Collectors

@Context
@Controller("/api/v1/dataplanes")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(bean = DataplaneCredentialsService::class)
open class DataplaneController(
  private val dataplaneService: DataplaneService,
  private val dataplaneGroupService: DataplaneGroupService,
) : DataplaneApi {
  @Post("/create")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createDataplane(
    @Body dataplaneCreateRequestBody: DataplaneCreateRequestBody,
  ): DataplaneCreateResponse {
    val dataplane =
      Dataplane().apply {
        dataplaneGroupId = dataplaneCreateRequestBody.dataplaneGroupId
        name = dataplaneCreateRequestBody.name
        enabled = dataplaneCreateRequestBody.enabled
      }
    val createdDataplane = dataplaneService.writeDataplane(dataplane)
    val dataplaneAuth = dataplaneService.createCredentials(createdDataplane.id)
    return DataplaneCreateResponse()
      .dataplaneId(createdDataplane.id)
      .clientId(dataplaneAuth.clientId)
      .clientSecret(dataplaneAuth.clientSecret)
  }

  @Post("/update")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDataplane(
    @Body dataplaneUpdateRequestBody: DataplaneUpdateRequestBody,
  ): DataplaneRead =
    toDataplaneRead(
      dataplaneService.updateDataplane(
        dataplaneUpdateRequestBody.dataplaneId,
        dataplaneUpdateRequestBody.name,
        dataplaneUpdateRequestBody.enabled,
      ),
    )

  @Post("/delete")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteDataplane(
    @Body dataplaneDeleteRequestBody: DataplaneDeleteRequestBody,
  ): DataplaneRead {
    val tombstonedDataplane = dataplaneService.deleteDataplane(dataplaneDeleteRequestBody.dataplaneId)

    return toDataplaneRead(tombstonedDataplane)
  }

  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDataplanes(
    @Body dataplaneListRequestBody: DataplaneListRequestBody,
  ): DataplaneListResponse {
    val dataplanes: List<Dataplane> = dataplaneService.listDataplanes(dataplaneListRequestBody.dataplaneGroupId)
    return DataplaneListResponse()
      .dataplanes(
        dataplanes
          .stream()
          .map { dataplane: Dataplane -> toDataplaneRead(dataplane) }
          .collect(Collectors.toList()),
      )
  }

  fun toDataplaneRead(dataplane: Dataplane): DataplaneRead {
    val dataplaneRead = DataplaneRead()
    dataplaneRead
      .dataplaneId(dataplane.id)
      .name(dataplane.name)
      .dataplaneGroupId(dataplane.dataplaneGroupId)
      .enabled(dataplane.enabled)
      .createdAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplane.createdAt), ZoneOffset.UTC))
      .updatedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplane.updatedAt), ZoneOffset.UTC))

    return dataplaneRead
  }

  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDataplaneToken(
    @Body dataplaneTokenRequest: DataplaneTokenRequestBody,
  ): AccessToken {
    val token =
      dataplaneService.getToken(
        dataplaneTokenRequest.clientId,
        dataplaneTokenRequest.clientSecret,
      )

    val accessToken = AccessToken()
    accessToken.accessToken = token
    return accessToken
  }

  @Post("/initialize")
  @Secured(ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun initializeDataplane(
    @Body req: DataplaneInitRequestBody,
  ): DataplaneInitResponse {
    val dataplane = dataplaneService.getDataplaneFromClientId(req.clientId)
    val dataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId)

    val resp = DataplaneInitResponse()
    resp.dataplaneName = dataplane.name
    resp.dataplaneId = dataplane.id
    resp.dataplaneEnabled = dataplane.enabled && dataplaneGroup.enabled
    resp.dataplaneGroupName = dataplaneGroup.name
    resp.dataplaneGroupId = dataplaneGroup.id

    return resp
  }

  @Post("/heartbeat")
  @Secured(ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun heartbeatDataplane(
    @Body req: DataplaneHeartbeatRequestBody,
  ): DataplaneHeartbeatResponse {
    val dataplane = dataplaneService.getDataplaneFromClientId(req.clientId)
    val dataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId)

    return DataplaneHeartbeatResponse().apply {
      dataplaneName = dataplane.name
      dataplaneId = dataplane.id
      dataplaneEnabled = dataplane.enabled && dataplaneGroup.enabled
      dataplaneGroupName = dataplaneGroup.name
      dataplaneGroupId = dataplaneGroup.id
    }
  }
}
