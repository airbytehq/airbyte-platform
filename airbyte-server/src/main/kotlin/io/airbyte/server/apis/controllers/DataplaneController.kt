/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataplaneApi
import io.airbyte.api.model.generated.DataplaneCreateRequestBody
import io.airbyte.api.model.generated.DataplaneCreateResponse
import io.airbyte.api.model.generated.DataplaneDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneGetIdRequestBody
import io.airbyte.api.model.generated.DataplaneListRequestBody
import io.airbyte.api.model.generated.DataplaneListResponse
import io.airbyte.api.model.generated.DataplaneRead
import io.airbyte.api.model.generated.DataplaneReadId
import io.airbyte.api.model.generated.DataplaneUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.DataplaneNameAlreadyExistsProblem
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Dataplane
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import org.jooq.exception.DataAccessException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.stream.Collectors
import kotlin.jvm.optionals.getOrNull
import io.airbyte.data.services.DataplaneService as DataDataplaneService
import io.airbyte.server.services.DataplaneService as ServerDataplaneService

@Controller("/api/v1/dataplanes")
@Secured(SecurityRule.IS_AUTHENTICATED)
class DataplaneController(
  private val dataDataplaneService: DataDataplaneService,
  private val serverDataplaneService: ServerDataplaneService,
  private val currentUserService: CurrentUserService,
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
        updatedBy = currentUserService.currentUserIdIfExists.getOrNull()
      }
    val createdDataplane = writeDataplane(dataplane)
    // Dummy response until auth is wired through
    return DataplaneCreateResponse().dataplaneId(createdDataplane.id)
  }

  @Post("/update")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDataplane(
    @Body dataplaneUpdateRequestBody: DataplaneUpdateRequestBody,
  ): DataplaneRead {
    val existingDataplane = dataDataplaneService.getDataplane(dataplaneUpdateRequestBody.dataplaneId)

    val updatedDataplane =
      existingDataplane.apply {
        name = dataplaneUpdateRequestBody.name
        enabled = dataplaneUpdateRequestBody.enabled
        updatedBy = currentUserService.currentUserIdIfExists.getOrNull()
      }

    return toDataplaneRead(writeDataplane(updatedDataplane))
  }

  @Post("/delete")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteDataplane(
    @Body dataplaneDeleteRequestBody: DataplaneDeleteRequestBody,
  ): DataplaneRead {
    val existingDataplane = dataDataplaneService.getDataplane(dataplaneDeleteRequestBody.dataplaneId)

    val tombstonedDataplane =
      existingDataplane.apply {
        tombstone = true
        updatedBy = currentUserService.currentUserIdIfExists.getOrNull()
      }

    return toDataplaneRead(writeDataplane(tombstonedDataplane))
  }

  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDataplanes(
    @Body dataplaneListRequestBody: DataplaneListRequestBody,
  ): DataplaneListResponse {
    val dataplanes: List<Dataplane> = dataDataplaneService.listDataplanes(dataplaneListRequestBody.dataplaneGroupId, false)
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
      .dataplaneId(dataplane.getId())
      .name(dataplane.getName())
      .dataplaneGroupId(dataplane.getDataplaneGroupId())
      .enabled(dataplane.getEnabled())
      .createdAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplane.getCreatedAt()), ZoneOffset.UTC))
      .updatedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplane.getUpdatedAt()), ZoneOffset.UTC))

    return dataplaneRead
  }

  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDataplaneId(dataplaneGetIdRequestBody: DataplaneGetIdRequestBody): DataplaneReadId {
    val connectionId = dataplaneGetIdRequestBody.connectionId
    val actorType = dataplaneGetIdRequestBody.actorType
    val actorId = dataplaneGetIdRequestBody.actorId
    val workspaceId = dataplaneGetIdRequestBody.workspaceId
    val queueName = serverDataplaneService.getQueueName(connectionId, actorType, actorId, workspaceId, dataplaneGetIdRequestBody.workloadPriority)

    return DataplaneReadId().id(queueName)
  }

  fun writeDataplane(dataplane: Dataplane): Dataplane {
    try {
      return dataDataplaneService.writeDataplane(dataplane)
    } catch (e: DataAccessException) {
      if (e.message?.contains("duplicate key value violates unique constraint") == true &&
        e.message?.contains("dataplane_dataplane_group_id_name_key") == true
      ) {
        throw DataplaneNameAlreadyExistsProblem()
      }
      throw e
    }
  }
}
