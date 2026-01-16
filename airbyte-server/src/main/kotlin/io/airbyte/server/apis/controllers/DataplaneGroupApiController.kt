/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.OrganizationId
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
  private val entitlementService: EntitlementService,
) : DataplaneGroupApi {
  @Post("/create")
  @RequiresIntent(Intent.ManageDataplaneGroups)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createDataplaneGroup(
    @Body dataplaneGroupCreateRequestBody: DataplaneGroupCreateRequestBody,
  ): DataplaneGroupRead? {
    entitlementService.ensureEntitled(OrganizationId(dataplaneGroupCreateRequestBody.organizationId), SelfManagedRegionsEntitlement)
    val createdDataplaneGroup =
      DataplaneGroup().apply {
        organizationId = dataplaneGroupCreateRequestBody.organizationId
        name = dataplaneGroupCreateRequestBody.name
        enabled = dataplaneGroupCreateRequestBody.enabled
      }
    return toDataplaneGroupRead(writeDataplaneGroup(createdDataplaneGroup))
  }

  @Post("/update")
  @RequiresIntent(Intent.ManageDataplaneGroups)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDataplaneGroup(
    @Body dataplaneGroupUpdateRequestBody: DataplaneGroupUpdateRequestBody,
  ): DataplaneGroupRead {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneGroupId(dataplaneGroupUpdateRequestBody.dataplaneGroupId))
    val updatedDataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplaneGroupUpdateRequestBody.dataplaneGroupId)

    val dataplaneGroup =
      updatedDataplaneGroup.apply {
        dataplaneGroupUpdateRequestBody.name?.let { name = it }
        dataplaneGroupUpdateRequestBody.enabled?.let { enabled = it }
      }

    return toDataplaneGroupRead(writeDataplaneGroup(dataplaneGroup))
  }

  @Post("/delete")
  @RequiresIntent(Intent.ManageDataplaneGroups)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteDataplaneGroup(
    @Body dataplaneGroupDeleteRequestBody: DataplaneGroupDeleteRequestBody,
  ): DataplaneGroupRead? {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneGroupId(dataplaneGroupDeleteRequestBody.dataplaneGroupId))
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
    val organizationId = dataplaneGroupListRequestBody.organizationId

    // Build list of organization IDs to query based on entitlements
    val organizationIds = mutableListOf(DEFAULT_ORGANIZATION_ID)

    // Check if organization has PrivateLink entitlement
    val hasPrivateLink =
      entitlementService
        .checkEntitlement(
          organizationId = OrganizationId(organizationId),
          entitlement = PrivateLinkEntitlement,
        ).isEntitled

    if (hasPrivateLink) {
      organizationIds.add(PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)
    }

    // Check if organization has SelfManagedRegions entitlement
    val hasSelfManagedRegions =
      entitlementService
        .checkEntitlement(
          organizationId = OrganizationId(organizationId),
          entitlement = SelfManagedRegionsEntitlement,
        ).isEntitled

    if (hasSelfManagedRegions) {
      organizationIds.add(organizationId)
    }

    val dataplaneGroups =
      dataplaneGroupService.listDataplaneGroups(
        organizationIds,
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

  private fun ensureManageDataplanesAndDataplaneGroupsEntitlement(dataplaneGroupId: DataplaneGroupId) {
    val orgId = OrganizationId(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId.value))
    entitlementService.ensureEntitled(orgId, SelfManagedRegionsEntitlement)
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
