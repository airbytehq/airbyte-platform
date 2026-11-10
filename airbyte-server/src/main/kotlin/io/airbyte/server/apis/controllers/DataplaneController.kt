/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.DataplaneApi
import io.airbyte.api.server.generated.apis.DataplanesApi
import io.airbyte.api.server.generated.models.AccessToken
import io.airbyte.api.server.generated.models.DataplaneCreateRequestBody
import io.airbyte.api.server.generated.models.DataplaneCreateResponse
import io.airbyte.api.server.generated.models.DataplaneDeleteRequestBody
import io.airbyte.api.server.generated.models.DataplaneDiagnosticRead
import io.airbyte.api.server.generated.models.DataplaneDiagnosticResult
import io.airbyte.api.server.generated.models.DataplaneHealthHistoryBucket
import io.airbyte.api.server.generated.models.DataplaneHealthListResponse
import io.airbyte.api.server.generated.models.DataplaneHealthRead
import io.airbyte.api.server.generated.models.DataplaneHeartbeatRequestBody
import io.airbyte.api.server.generated.models.DataplaneHeartbeatResponse
import io.airbyte.api.server.generated.models.DataplaneInitRequestBody
import io.airbyte.api.server.generated.models.DataplaneInitResponse
import io.airbyte.api.server.generated.models.DataplaneListRequestBody
import io.airbyte.api.server.generated.models.DataplaneListResponse
import io.airbyte.api.server.generated.models.DataplaneRead
import io.airbyte.api.server.generated.models.DataplaneSecretStorage
import io.airbyte.api.server.generated.models.DataplaneSecretStoragesRequestBody
import io.airbyte.api.server.generated.models.DataplaneSecretStoragesResponse
import io.airbyte.api.server.generated.models.DataplaneTokenRequestBody
import io.airbyte.api.server.generated.models.DataplaneUpdateRequestBody
import io.airbyte.api.server.generated.models.HeartbeatRecord
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.constants.ApiConstants.AIRBYTE_VERSION_HEADER
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.WorkloadConstants.Companion.PUBLIC_ORG_ID
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.ServiceAccountNotFound
import io.airbyte.domain.models.DataplaneDiagnostic
import io.airbyte.domain.models.DataplaneDiagnosticKind
import io.airbyte.domain.models.DataplaneDiagnosticOutcome
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.DataplaneId
import io.airbyte.domain.models.DataplaneVersionSupport
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SavedDataplaneDiagnostic
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.featureflag.DataplaneHealthUi
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.server.services.DataplaneService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.HeaderParam
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.stream.Collectors
import io.airbyte.api.server.generated.models.DataplaneDiagnosticKind as ApiDataplaneDiagnosticKind
import io.airbyte.api.server.generated.models.DataplaneDiagnosticStatus as ApiDataplaneDiagnosticStatus
import io.airbyte.api.server.generated.models.DataplaneVersionSupport as ApiDataplaneVersionSupport
import io.airbyte.api.server.generated.models.ScopeType as ApiScopeType

private val logger = KotlinLogging.logger {}

// Size of dataplane_diagnostic.error_code. A longer code would fail the save and the whole heartbeat.
private const val MAX_DIAGNOSTIC_ERROR_CODE_LENGTH = 256

@Context
@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataplaneController(
  private val dataplaneService: DataplaneService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataplaneHealthService: DataplaneHealthService,
  private val entitlementService: EntitlementService,
  private val metricClient: MetricClient,
  private val airbyteConfig: AirbyteConfig,
  private val featureFlagClient: FeatureFlagClient,
  private val secretStorageService: SecretStorageService,
) : DataplaneApi,
  DataplanesApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneCreateRule::class,
  )
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
    val dataplaneWithServiceAccount = dataplaneService.createNewDataplane(dataplane, dataplaneCreateRequestBody.instanceScope ?: false)

    return DataplaneCreateResponse(
      dataplaneId = dataplaneWithServiceAccount.dataplane.id,
      regionId = dataplaneWithServiceAccount.dataplane.dataplaneGroupId,
      clientId = dataplaneWithServiceAccount.serviceAccount.id.toString(),
      clientSecret = dataplaneWithServiceAccount.serviceAccount.secret,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneManageRule::class,
  )
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneManageRule::class,
  )
  override fun deleteDataplane(
    @Body dataplaneDeleteRequestBody: DataplaneDeleteRequestBody,
  ): DataplaneRead {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneId(dataplaneDeleteRequestBody.dataplaneId))
    val tombstonedDataplane = dataplaneService.deleteDataplane(dataplaneDeleteRequestBody.dataplaneId)

    return toDataplaneRead(tombstonedDataplane)
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneManageRule::class,
  )
  override fun listDataplanes(
    @Body dataplaneListRequestBody: DataplaneListRequestBody,
  ): DataplaneListResponse {
    ensureManageDataplanesAndDataplaneGroupsEntitlement(DataplaneGroupId(dataplaneListRequestBody.dataplaneGroupId))
    val dataplanes: List<Dataplane> = dataplaneService.listDataplanes(dataplaneListRequestBody.dataplaneGroupId)
    return DataplaneListResponse(
      dataplanes =
        dataplanes
          .stream()
          .map { dataplane: Dataplane -> toDataplaneRead(dataplane) }
          .collect(Collectors.toList()),
    )
  }

  fun toDataplaneRead(dataplane: Dataplane): DataplaneRead =
    DataplaneRead(
      dataplaneId = dataplane.id,
      name = dataplane.name,
      dataplaneGroupId = dataplane.dataplaneGroupId,
      enabled = dataplane.enabled,
      createdAt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplane.createdAt), ZoneOffset.UTC),
      updatedAt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(dataplane.updatedAt), ZoneOffset.UTC),
    )

  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.LOGIN,
  )
  override fun getDataplaneToken(
    @Body dataplaneTokenRequest: DataplaneTokenRequestBody,
  ): AccessToken {
    val token =
      dataplaneService.getToken(
        dataplaneTokenRequest.clientId,
        dataplaneTokenRequest.clientSecret,
      )

    return AccessToken(accessToken = token)
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneMachineRule::class,
  )
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

    // TODO implement version based gating of dataplane based on airbyte version here

    val resp =
      DataplaneInitResponse(
        dataplaneName = dataplane.name,
        dataplaneId = dataplane.id,
        dataplaneEnabled = dataplane.enabled && dataplaneGroup.enabled,
        dataplaneGroupName = dataplaneGroup.name,
        dataplaneGroupId = dataplaneGroup.id,
        organizationId = dataplaneGroup.organizationId,
      )

    reportDataplaneMetric(OssMetricsRegistry.DATAPLANE_INITIALIZE, dataplane, dataplaneGroup, dataplaneVersion = xAirbyteVersion)

    return resp
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneMachineRule::class,
  )
  override fun heartbeatDataplane(
    @Body req: DataplaneHeartbeatRequestBody,
    @HeaderParam(AIRBYTE_VERSION_HEADER) xAirbyteVersion: String?,
  ): DataplaneHeartbeatResponse {
    val dataplane = dataplaneService.getDataplaneByServiceAccountId(req.clientId)
    if (dataplane == null) {
      throw ServiceAccountNotFound(UUID.fromString(req.clientId))
    }

    val dataplaneGroup = dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId)

    // TODO implement version based gating of dataplane based on airbyte version here

    reportDataplaneMetric(OssMetricsRegistry.DATAPLANE_HEARTBEAT, dataplane, dataplaneGroup, dataplaneVersion = xAirbyteVersion)

    dataplaneHealthService.recordHeartbeat(
      dataplaneId = dataplane.id,
      controlPlaneVersion = airbyteConfig.version,
      dataplaneVersion = xAirbyteVersion,
    )

    val diagnostics = req.diagnostics.orEmpty().mapNotNull { toDataplaneDiagnostic(it, dataplane.id) }
    dataplaneHealthService.recordDiagnostics(
      dataplaneId = dataplane.id,
      organizationId = dataplaneGroup.organizationId,
      diagnostics = diagnostics,
    )

    return DataplaneHeartbeatResponse(
      dataplaneName = dataplane.name,
      dataplaneId = dataplane.id,
      dataplaneEnabled = dataplane.enabled && dataplaneGroup.enabled,
      dataplaneGroupName = dataplaneGroup.name,
      dataplaneGroupId = dataplaneGroup.id,
      organizationId = dataplaneGroup.organizationId,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneMachineRule::class,
  )
  override fun listDataplaneSecretStorages(
    @Body req: DataplaneSecretStoragesRequestBody,
  ): DataplaneSecretStoragesResponse {
    val secretStorages = secretStorageService.listForDataplane(UUID.fromString(req.clientId))

    return DataplaneSecretStoragesResponse(
      secretStorages =
        secretStorages.map {
          DataplaneSecretStorage(
            secretStorageId = it.id.value,
            scopeType =
              when (it.scopeType) {
                SecretStorageScopeType.ORGANIZATION -> ApiScopeType.ORGANIZATION
                SecretStorageScopeType.WORKSPACE -> ApiScopeType.WORKSPACE
              },
            scopeId = it.scopeId,
          )
        },
    )
  }

  /**
   * Returns null for a result the data plane filled in wrong, so one bad check never stops the check-in from being recorded.
   */
  private fun toDataplaneDiagnostic(
    result: DataplaneDiagnosticResult,
    dataplaneId: UUID,
  ): DataplaneDiagnostic? {
    val errorCode = result.errorCode
    val errorMessage = result.errorMessage
    val hasError = !errorCode.isNullOrBlank() && !errorMessage.isNullOrBlank()
    val outcome =
      when (result.status) {
        ApiDataplaneDiagnosticStatus.PASSED -> DataplaneDiagnosticOutcome.Passed
        ApiDataplaneDiagnosticStatus.FAILED -> if (hasError) DataplaneDiagnosticOutcome.Failed(errorCode, errorMessage) else null
        ApiDataplaneDiagnosticStatus.UNABLE_TO_COMPLETE ->
          if (hasError) DataplaneDiagnosticOutcome.UnableToComplete(errorCode, errorMessage) else null
      }
    if (outcome == null) {
      logger.warn {
        "Skipping ${result.kind} diagnostic for dataplane $dataplaneId, target ${result.targetId}: status ${result.status} needs error_code and error_message"
      }
      return null
    }
    if (errorCode.orEmpty().length > MAX_DIAGNOSTIC_ERROR_CODE_LENGTH) {
      logger.warn {
        "Skipping ${result.kind} diagnostic for dataplane $dataplaneId, target ${result.targetId}: error_code is longer than $MAX_DIAGNOSTIC_ERROR_CODE_LENGTH characters"
      }
      return null
    }

    val kind =
      when (result.kind) {
        ApiDataplaneDiagnosticKind.SECRET_STORAGE -> DataplaneDiagnosticKind.SECRET_STORAGE
      }
    val observation = result.observation.orEmpty()
    val missingKeys = requiredObservationKeys(kind).filter { it !in observation }
    if (missingKeys.isNotEmpty()) {
      logger.warn { "Skipping $kind diagnostic for dataplane $dataplaneId, target ${result.targetId}: observation is missing $missingKeys" }
      return null
    }

    return DataplaneDiagnostic(
      kind = kind,
      targetId = result.targetId,
      outcome = outcome,
      checkedAt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(result.checkedAt), ZoneOffset.UTC),
      observation = observation,
    )
  }

  private fun requiredObservationKeys(kind: DataplaneDiagnosticKind): List<String> =
    when (kind) {
      DataplaneDiagnosticKind.SECRET_STORAGE -> listOf("scope_type", "scope_id")
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = io.airbyte.server.auth.DataplaneManageRule::class,
  )
  override fun listDataplaneHealth(
    @Body request: OrganizationIdRequestBody,
  ): DataplaneHealthListResponse {
    entitlementService.ensureEntitled(OrganizationId(request.organizationId), SelfManagedRegionsEntitlement)

    val dataplaneGroups = dataplaneGroupService.listDataplaneGroups(listOf(request.organizationId), false)
    val dataplaneGroupMap = dataplaneGroups.associateBy { it.id }

    val dataplanes = dataplaneService.listDataplanes(dataplaneGroups.map { it.id })
    val dataplaneMap = dataplanes.associateBy { it.id }

    val healthUiEnabled = featureFlagClient.boolVariation(DataplaneHealthUi, Organization(request.organizationId))
    val healthInfos = dataplaneHealthService.getDataplaneHealthInfos(dataplanes.map { it.id }, healthUiEnabled)
    val diagnostics = dataplaneHealthService.getDiagnostics(request.organizationId, dataplanes.map { it.id })
    val diagnosticsByDataplane = diagnostics.groupBy { it.dataplaneId }
    val secretStorageNames =
      secretStorageService.getNamesForOrganization(
        OrganizationId(request.organizationId),
        diagnostics
          .filter { it.diagnostic.kind == DataplaneDiagnosticKind.SECRET_STORAGE }
          .map { SecretStorageId(it.diagnostic.targetId) }
          .toSet(),
      )
    val releaseSupportByVersion =
      if (healthUiEnabled) {
        dataplaneHealthService.getPublishedReleaseSupport(healthInfos.mapNotNull { it.dataplaneVersion }.toSet())
      } else {
        emptyMap()
      }

    val dataplaneHealthReads =
      healthInfos.map { healthInfo ->
        val dataplane = dataplaneMap[healthInfo.dataplaneId]!!
        val dataplaneGroup = dataplaneGroupMap[dataplane.dataplaneGroupId]!!
        val savedDiagnostics = diagnosticsByDataplane[healthInfo.dataplaneId].orEmpty()

        DataplaneHealthRead(
          dataplaneId = healthInfo.dataplaneId,
          dataplaneName = dataplane.name,
          dataplaneGroupId = dataplane.dataplaneGroupId,
          organizationId = dataplaneGroup.organizationId,
          status = DataplaneHealthRead.Status.entries.first { it.value == healthInfo.status.name },
          lastHeartbeatTimestamp = healthInfo.lastHeartbeatTimestamp?.toInstant()?.toEpochMilli(),
          recentHeartbeats =
            healthInfo.recentHeartbeats.map { hb ->
              HeartbeatRecord(timestamp = hb.timestamp.toInstant().toEpochMilli())
            },
          controlPlaneVersion = healthInfo.controlPlaneVersion,
          dataplaneVersion = healthInfo.dataplaneVersion,
          dataplaneVersionSupport =
            if (healthUiEnabled) {
              toVersionSupportRead(healthInfo.dataplaneVersion?.let { releaseSupportByVersion[it] } ?: DataplaneVersionSupport.Unknown)
            } else {
              null
            },
          enabled = dataplane.enabled && dataplaneGroup.enabled,
          healthHistory =
            if (healthUiEnabled) {
              healthInfo.healthHistory.map { bucket ->
                DataplaneHealthHistoryBucket(
                  bucketStart = bucket.bucketStart.toInstant().toEpochMilli(),
                  status = DataplaneHealthHistoryBucket.Status.entries.first { it.value == bucket.status.name },
                )
              }
            } else {
              null
            },
          diagnostics = savedDiagnostics.map { toDiagnosticRead(it, secretStorageNames) },
          unsupportedDiagnosticKinds =
            dataplaneHealthService.unsupportedDiagnosticKinds(healthInfo.dataplaneVersion, savedDiagnostics).map { toApiKind(it) },
        )
      }

    return DataplaneHealthListResponse(dataplanes = dataplaneHealthReads)
  }

  private fun toVersionSupportRead(support: DataplaneVersionSupport): ApiDataplaneVersionSupport =
    when (support) {
      is DataplaneVersionSupport.Published ->
        ApiDataplaneVersionSupport(
          status = if (support.supported) ApiDataplaneVersionSupport.Status.SUPPORTED else ApiDataplaneVersionSupport.Status.SUPPORT_ENDED,
          publishedAt = support.publishedAt.toInstant().toEpochMilli(),
          supportedUntil = support.supportedUntil.toInstant().toEpochMilli(),
        )
      DataplaneVersionSupport.Unknown -> ApiDataplaneVersionSupport(status = ApiDataplaneVersionSupport.Status.UNKNOWN)
    }

  private fun toApiKind(kind: DataplaneDiagnosticKind): ApiDataplaneDiagnosticKind =
    when (kind) {
      DataplaneDiagnosticKind.SECRET_STORAGE -> ApiDataplaneDiagnosticKind.SECRET_STORAGE
    }

  private fun toDiagnosticRead(
    saved: SavedDataplaneDiagnostic,
    secretStorageNames: Map<SecretStorageId, String>,
  ): DataplaneDiagnosticRead {
    val diagnostic = saved.diagnostic
    val (status, errorCode, errorMessage) =
      when (val outcome = diagnostic.outcome) {
        DataplaneDiagnosticOutcome.Passed -> Triple(ApiDataplaneDiagnosticStatus.PASSED, null, null)
        is DataplaneDiagnosticOutcome.Failed -> Triple(ApiDataplaneDiagnosticStatus.FAILED, outcome.errorCode, outcome.errorMessage)
        is DataplaneDiagnosticOutcome.UnableToComplete ->
          Triple(ApiDataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, outcome.errorCode, outcome.errorMessage)
      }

    return DataplaneDiagnosticRead(
      kind = toApiKind(diagnostic.kind),
      targetId = diagnostic.targetId,
      targetName =
        when (diagnostic.kind) {
          DataplaneDiagnosticKind.SECRET_STORAGE -> secretStorageNames[SecretStorageId(diagnostic.targetId)]
        },
      status = status,
      checkedAt = diagnostic.checkedAt.toInstant().toEpochMilli(),
      updatedAt = saved.updatedAt.toInstant().toEpochMilli(),
      errorCode = errorCode,
      errorMessage = errorMessage,
      observation = diagnostic.observation,
    )
  }
}
