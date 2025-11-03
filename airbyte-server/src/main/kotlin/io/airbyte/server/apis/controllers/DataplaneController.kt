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
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants.DATAPLANE
import io.airbyte.commons.constants.ApiConstants.AIRBYTE_VERSION_HEADER
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.WorkloadConstants.Companion.PUBLIC_ORG_ID
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.ServiceAccountNotFound
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.DataplaneId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.server.services.DataplaneService
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.HeaderParam
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.stream.Collectors

@Context
@Controller("/api/v1/dataplanes")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataplaneController(
  private val dataplaneService: DataplaneService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataplaneHealthService: DataplaneHealthService,
  private val roleResolver: RoleResolver,
  private val entitlementService: EntitlementService,
  private val metricClient: MetricClient,
  private val airbyteConfig: AirbyteConfig,
) : DataplaneApi {
  @Post("/create")
  @RequiresIntent(Intent.ManageDataplanes)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createDataplane(
    @Body dataplaneCreateRequestBody: DataplaneCreateRequestBody,
  ): DataplaneCreateResponse {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneGroupId(dataplaneCreateRequestBody.dataplaneGroupId))
    val dataplane =
      Dataplane().apply {
        id = UUID.randomUUID()
        dataplaneGroupId = dataplaneCreateRequestBody.dataplaneGroupId
        name = dataplaneCreateRequestBody.name
        enabled = dataplaneCreateRequestBody.enabled
      }

    // this function will also create a service account
    // for the dataplane and grant that account the appropriate permissions.
    val dataplaneWithServiceAccount = dataplaneService.createNewDataplane(dataplane, dataplaneCreateRequestBody.instanceScope)

    return DataplaneCreateResponse()
      .dataplaneId(dataplaneWithServiceAccount.dataplane.id)
      .clientId(dataplaneWithServiceAccount.serviceAccount.id.toString())
      .clientSecret(dataplaneWithServiceAccount.serviceAccount.secret)
  }

  @Post("/update")
  @RequiresIntent(Intent.ManageDataplanes)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDataplane(
    @Body dataplaneUpdateRequestBody: DataplaneUpdateRequestBody,
  ): DataplaneRead {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneId(dataplaneUpdateRequestBody.dataplaneId))
    return toDataplaneRead(
      dataplaneService.updateDataplane(
        dataplaneUpdateRequestBody.dataplaneId,
        dataplaneUpdateRequestBody.name,
        dataplaneUpdateRequestBody.enabled,
      ),
    )
  }

  @Post("/delete")
  @RequiresIntent(Intent.ManageDataplanes)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteDataplane(
    @Body dataplaneDeleteRequestBody: DataplaneDeleteRequestBody,
  ): DataplaneRead {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneId(dataplaneDeleteRequestBody.dataplaneId))
    val tombstonedDataplane = dataplaneService.deleteDataplane(dataplaneDeleteRequestBody.dataplaneId)

    return toDataplaneRead(tombstonedDataplane)
  }

  @Post("/list")
  @RequiresIntent(Intent.ManageDataplanes)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDataplanes(
    @Body dataplaneListRequestBody: DataplaneListRequestBody,
  ): DataplaneListResponse {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneGroupId(dataplaneListRequestBody.dataplaneGroupId))
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
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun initializeDataplane(
    @Body req: DataplaneInitRequestBody,
    @HeaderParam(AIRBYTE_VERSION_HEADER) xAirbyteVersion: String?,
  ): DataplaneInitResponse {
    // Note: after the service account migration, the service account id is the same as the
    // old client id from the dataplane_client_credentials table. If the service account is not
    // from a migration, then the clientId here should simply be the service account id
    val dataplane = dataplaneService.getDataplaneByServiceAccountId(req.clientId)
    if (dataplane == null) {
      throw ServiceAccountNotFound(UUID.fromString(req.clientId))
    }
    val dataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId)

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withOrg(dataplaneGroup.organizationId)
      .requireRole(DATAPLANE)

    // TODO implement version based gating of dataplane based on airbyte version here

    val resp = DataplaneInitResponse()
    resp.dataplaneName = dataplane.name
    resp.dataplaneId = dataplane.id
    resp.dataplaneEnabled = dataplane.enabled && dataplaneGroup.enabled
    resp.dataplaneGroupName = dataplaneGroup.name
    resp.dataplaneGroupId = dataplaneGroup.id
    resp.organizationId = dataplaneGroup.organizationId

    reportDataplaneMetric(OssMetricsRegistry.DATAPLANE_INITIALIZE, dataplane, dataplaneGroup, dataplaneVersion = xAirbyteVersion)

    return resp
  }

  @Post("/heartbeat")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun heartbeatDataplane(
    @Body req: DataplaneHeartbeatRequestBody,
    @HeaderParam(AIRBYTE_VERSION_HEADER) xAirbyteVersion: String?,
  ): DataplaneHeartbeatResponse {
    val dataplane = dataplaneService.getDataplaneByServiceAccountId(req.clientId)
    if (dataplane == null) {
      throw ServiceAccountNotFound(UUID.fromString(req.clientId))
    }

    val dataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId)

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withOrg(dataplaneGroup.organizationId)
      .requireRole(DATAPLANE)

    // TODO implement version based gating of dataplane based on airbyte version here

    reportDataplaneMetric(OssMetricsRegistry.DATAPLANE_HEARTBEAT, dataplane, dataplaneGroup, dataplaneVersion = xAirbyteVersion)

    dataplaneHealthService.recordHeartbeat(
      dataplaneId = dataplane.id,
      controlPlaneVersion = airbyteConfig.version,
      dataplaneVersion = xAirbyteVersion,
    )

    return DataplaneHeartbeatResponse().apply {
      dataplaneName = dataplane.name
      dataplaneId = dataplane.id
      dataplaneEnabled = dataplane.enabled && dataplaneGroup.enabled
      dataplaneGroupName = dataplaneGroup.name
      dataplaneGroupId = dataplaneGroup.id
      organizationId = dataplaneGroup.organizationId
    }
  }

  private fun reportDataplaneMetric(
    metric: OssMetricsRegistry,
    dataplane: Dataplane,
    dataplaneGroup: DataplaneGroup,
    dataplaneVersion: String?,
  ) {
    metricClient.count(
      metric,
      1,
      MetricAttribute(MetricTags.DATA_PLANE_ID_TAG, dataplane.id.toString()),
      MetricAttribute(MetricTags.DATA_PLANE_NAME_TAG, dataplane.name),
      MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, dataplaneGroup.id.toString()),
      MetricAttribute(MetricTags.DATA_PLANE_GROUP_NAME_TAG, dataplaneGroup.name),
      MetricAttribute(MetricTags.DATA_PLANE_VERSION, dataplaneVersion ?: MetricTags.UNKNOWN),
      MetricAttribute(
        MetricTags.DATA_PLANE_VISIBILITY,
        if (dataplaneGroup.organizationId == PUBLIC_ORG_ID) MetricTags.PUBLIC else MetricTags.PRIVATE,
      ),
      MetricAttribute(MetricTags.DATA_PLANE_ORG_ID, dataplaneGroup.organizationId.toString()),
    )
  }

  private fun ensureManageDataplanesAndDataplaneGroupsEntitlement(dataplaneGroupId: DataplaneGroupId) {
    val orgId = OrganizationId(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId.value))
    entitlementService.ensureEntitled(orgId, SelfManagedRegionsEntitlement)
  }

  private fun ensureManageDataplanesAndDataplaneGroupsEntitlement(dataplaneId: DataplaneId) {
    val dataplaneGroupId = dataplaneService.getDataplane(dataplaneId.value.toString()).dataplaneGroupId
    val orgId = OrganizationId(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId))
    entitlementService.ensureEntitled(orgId, SelfManagedRegionsEntitlement)
  }
}
