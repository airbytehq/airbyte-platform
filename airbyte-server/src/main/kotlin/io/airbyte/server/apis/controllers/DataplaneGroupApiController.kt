/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataplaneGroupApi
import io.airbyte.api.model.generated.DataplaneGroupCreateRequestBody
import io.airbyte.api.model.generated.DataplaneGroupDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.model.generated.DataplaneGroupListResponse
import io.airbyte.api.model.generated.DataplaneGroupRead
import io.airbyte.api.model.generated.DataplaneGroupUpdateRequestBody
import io.airbyte.api.model.generated.DataplaneRead
import io.airbyte.api.problems.throwable.generated.DataplaneGroupNameAlreadyExistsProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.server.services.DataplaneService
import io.micronaut.context.annotation.Context
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

@Controller("/api/v1/dataplane_group")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class DataplaneGroupApiController(
  protected val dataplaneGroupService: DataplaneGroupService,
  protected val dataplaneService: DataplaneService,
) : DataplaneGroupApi {
  @Post("/create")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createDataplaneGroup(
    @Body dataplaneGroupCreateRequestBody: DataplaneGroupCreateRequestBody,
  ): DataplaneGroupRead? {
    val createdDataplaneGroup =
      DataplaneGroup().apply {
        organizationId = dataplaneGroupCreateRequestBody.organizationId
        name = dataplaneGroupCreateRequestBody.name
        enabled = dataplaneGroupCreateRequestBody.enabled
      }
    return toDataplaneGroupRead(writeDataplaneGroup(createdDataplaneGroup))
  }

  @Post("/update")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDataplaneGroup(
    @Body dataplaneGroupUpdateRequestBody: DataplaneGroupUpdateRequestBody,
  ): DataplaneGroupRead {
    val updatedDataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplaneGroupUpdateRequestBody.dataplaneGroupId)

    val dataplaneGroup =
      updatedDataplaneGroup.apply {
        dataplaneGroupUpdateRequestBody.name?.let { name = it }
        dataplaneGroupUpdateRequestBody.enabled?.let { enabled = it }
      }

    return toDataplaneGroupRead(writeDataplaneGroup(dataplaneGroup))
  }

  @Post("/delete")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteDataplaneGroup(
    @Body dataplaneGroupDeleteRequestBody: DataplaneGroupDeleteRequestBody,
  ): DataplaneGroupRead? {
    val deletedDataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplaneGroupDeleteRequestBody.dataplaneGroupId)
    val tombstonedGroup =
      deletedDataplaneGroup.apply {
        tombstone = true
      }
    dataplaneService.listDataplanes(dataplaneGroupDeleteRequestBody.dataplaneGroupId).forEach {
      dataplaneService.deleteDataplane(it.id)
    }
    return toDataplaneGroupRead(writeDataplaneGroup(tombstonedGroup))
  }

  @Post("/list")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDataplaneGroups(
    @Body dataplaneGroupListRequestBody: DataplaneGroupListRequestBody,
  ): DataplaneGroupListResponse? {
    val dataplaneGroups =
      dataplaneGroupService.listDataplaneGroups(
        listOf(DEFAULT_ORGANIZATION_ID, dataplaneGroupListRequestBody.organizationId),
        false,
      )
    return DataplaneGroupListResponse()
      .dataplaneGroups(
        dataplaneGroups.map { dataplaneGroup ->
          this.toDataplaneGroupRead(dataplaneGroup)
        },
      )
  }

  private fun toDataplaneGroupRead(dataplaneGroup: DataplaneGroup): DataplaneGroupRead {
    val dataplaneGroupRead = DataplaneGroupRead()
    dataplaneGroupRead
      .dataplaneGroupId(dataplaneGroup.id)
      .name(dataplaneGroup.name)
      .organizationId(dataplaneGroup.organizationId)
      .enabled(dataplaneGroup.enabled)
      .createdAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplaneGroup.createdAt), ZoneOffset.UTC))
      .updatedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplaneGroup.updatedAt), ZoneOffset.UTC))
      .dataplanes(
        dataplaneService.listDataplanes(dataplaneGroup.id).map {
          DataplaneRead().apply {
            dataplaneId = it.id
            dataplaneGroupId = it.dataplaneGroupId
            name = it.name
            enabled = it.enabled
            createdAt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(it.createdAt), ZoneOffset.UTC)
            updatedAt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(it.updatedAt), ZoneOffset.UTC)
          }
        },
      )
    return dataplaneGroupRead
  }

  fun writeDataplaneGroup(dataplaneGroup: DataplaneGroup): DataplaneGroup {
    try {
      return dataplaneGroupService.writeDataplaneGroup(dataplaneGroup)
    } catch (e: DataAccessException) {
      if (e.message?.contains("duplicate key value violates unique constraint") == true &&
        e.message?.contains("dataplane_group_organization_id_name_key") == true
      ) {
        throw DataplaneGroupNameAlreadyExistsProblem()
      }
      throw e
    }
  }
}
