/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.DataplaneDeleteRequestBody
import io.airbyte.api.server.generated.models.DataplaneDiagnosticRead
import io.airbyte.api.server.generated.models.DataplaneDiagnosticResult
import io.airbyte.api.server.generated.models.DataplaneDiagnosticStatus
import io.airbyte.api.server.generated.models.DataplaneHealthHistoryBucket
import io.airbyte.api.server.generated.models.DataplaneHealthRead
import io.airbyte.api.server.generated.models.DataplaneHeartbeatRequestBody
import io.airbyte.api.server.generated.models.DataplaneHeartbeatResponse
import io.airbyte.api.server.generated.models.DataplaneInitRequestBody
import io.airbyte.api.server.generated.models.DataplaneInitResponse
import io.airbyte.api.server.generated.models.DataplaneListRequestBody
import io.airbyte.api.server.generated.models.DataplaneTokenRequestBody
import io.airbyte.api.server.generated.models.DataplaneUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.airbyte.domain.models.DataplaneDiagnostic
import io.airbyte.domain.models.DataplaneDiagnosticKind
import io.airbyte.domain.models.DataplaneDiagnosticOutcome
import io.airbyte.domain.models.DataplaneHealthInfo
import io.airbyte.domain.models.DataplaneVersionSupport
import io.airbyte.domain.models.HealthHistoryBucket
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SavedDataplaneDiagnostic
import io.airbyte.featureflag.DataplaneHealthUi
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import io.airbyte.api.server.generated.models.DataplaneDiagnosticKind as ApiDataplaneDiagnosticKind
import io.airbyte.api.server.generated.models.DataplaneVersionSupport as ApiDataplaneVersionSupport
import io.airbyte.server.services.DataplaneService as ServerDataplaneService

class DataplaneControllerTest {
  companion object {
    private const val TEST_DATAPLANE_VERSION = "test-123"
    private const val TEST_CONTROL_PLANE_VERSION = "1.0.0"
    private val CHECKED_AT = OffsetDateTime.of(2026, 11, 3, 10, 0, 0, 0, ZoneOffset.UTC)

    private val dataplaneService = mockk<ServerDataplaneService>()
    private val dataplaneGroupService = mockk<DataplaneGroupService>()
    private val dataplaneHealthService =
      mockk<DataplaneHealthService>(relaxed = true).apply {
        every { getPublishedReleaseSupport(any(), any()) } returns emptyMap()
      }

    private val entitlementService = mockk<EntitlementService>(relaxed = true)
    private val airbyteConfig =
      mockk<AirbyteConfig>().apply {
        every { version } returns TEST_CONTROL_PLANE_VERSION
      }
    private val featureFlagClient =
      mockk<FeatureFlagClient>().apply {
        every { boolVariation(DataplaneHealthUi, any()) } returns false
      }
    private val dataplaneController =
      DataplaneController(
        dataplaneService,
        dataplaneGroupService,
        dataplaneHealthService,
        entitlementService,
        mockk(relaxed = true),
        airbyteConfig,
        featureFlagClient,
      )
    private val MOCK_DATAPLANE_GROUP_ID = UUID.randomUUID()
  }

  @Test
  fun `updateDataplane returns the dataplane`() {
    val mockDataplane = createDataplane()
    val newName = "new name"
    val newEnabled = true
    val mockOrganizationId = UUID.randomUUID()

    every { dataplaneService.getDataplane(mockDataplane.id.toString()) } returns mockDataplane
    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplane.dataplaneGroupId) } returns mockOrganizationId
    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneService.updateDataplane(any(), any(), any()) } returns
      mockDataplane.apply {
        name = newName
        enabled = newEnabled
      }

    val updatedDataplane =
      dataplaneController.updateDataplane(
        DataplaneUpdateRequestBody(dataplaneId = mockDataplane.id, name = newName, enabled = newEnabled),
      )

    Assertions.assertEquals(mockDataplane.id, updatedDataplane.dataplaneId)
    Assertions.assertEquals(newName, updatedDataplane.name)
    Assertions.assertEquals(newEnabled, updatedDataplane.enabled)
  }

  @Test
  fun `deleteDataplane tombstones dataplane`() {
    val mockDataplane = createDataplane()
    val mockOrganizationId = UUID.randomUUID()

    every { dataplaneService.getDataplane(mockDataplane.id.toString()) } returns mockDataplane
    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplane.dataplaneGroupId) } returns mockOrganizationId
    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneService.deleteDataplane(any()) } returns mockDataplane

    val dataplaneDeleteRequestBody =
      DataplaneDeleteRequestBody(dataplaneId = mockDataplane.id)

    dataplaneController.deleteDataplane(dataplaneDeleteRequestBody)

    verify { dataplaneService.deleteDataplane(mockDataplane.id) }
  }

  @Test
  fun `listDataplanes returns dataplanes`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()
    val mockOrganizationId = UUID.randomUUID()

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(MOCK_DATAPLANE_GROUP_ID) } returns mockOrganizationId
    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneService.listDataplanes(MOCK_DATAPLANE_GROUP_ID) } returns
      listOf(
        createDataplane(dataplaneId1),
        createDataplane(dataplaneId2),
      )
    val dataplanes = dataplaneController.listDataplanes(DataplaneListRequestBody(dataplaneGroupId = MOCK_DATAPLANE_GROUP_ID))
    val responseDataplanes = dataplanes.dataplanes!!

    Assertions.assertEquals(2, responseDataplanes.size)
    Assertions.assertEquals(dataplaneId1, responseDataplanes[0].dataplaneId)
    Assertions.assertEquals(dataplaneId2, responseDataplanes[1].dataplaneId)
  }

  @Test
  fun `getDataplaneToken returns a valid token`() {
    val clientId = "test-client-id"
    val clientSecret = "test-client-secret"
    val expectedToken = "test-token"

    every { dataplaneService.getToken(clientId, clientSecret) } returns expectedToken

    val requestBody = DataplaneTokenRequestBody(clientId = clientId, clientSecret = clientSecret)
    val accessToken = dataplaneController.getDataplaneToken(requestBody)

    Assertions.assertEquals(expectedToken, accessToken.accessToken)
  }

  @Test
  fun `heartbeatDataplane returns expected dataplane information`() {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)
    val dataplaneGroup = createDataplaneGroup(dataplane.dataplaneGroupId)

    every { dataplaneService.getDataplaneByServiceAccountId(clientId) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId) } returns dataplaneGroup

    val req = DataplaneHeartbeatRequestBody(clientId = clientId)

    val result = dataplaneController.heartbeatDataplane(req, TEST_DATAPLANE_VERSION)

    val expected =
      DataplaneHeartbeatResponse(
        dataplaneName = dataplane.name,
        dataplaneId = dataplane.id,
        dataplaneEnabled = dataplane.enabled,
        dataplaneGroupName = dataplaneGroup.name,
        dataplaneGroupId = dataplaneGroup.id,
        organizationId = dataplaneGroup.organizationId,
      )

    Assertions.assertEquals(expected, result)

    verify {
      dataplaneHealthService.recordHeartbeat(
        dataplaneId = dataplaneId,
        controlPlaneVersion = TEST_CONTROL_PLANE_VERSION,
        dataplaneVersion = TEST_DATAPLANE_VERSION,
      )
    }
  }

  @Test
  fun `initializeDataplane returns information needed for dataplane initialization`() {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)
    val dataplaneGroup = createDataplaneGroup(dataplane.dataplaneGroupId)

    every { dataplaneService.getDataplaneByServiceAccountId(clientId) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId) } returns dataplaneGroup

    val req = DataplaneInitRequestBody(clientId = clientId)

    val result = dataplaneController.initializeDataplane(req, TEST_DATAPLANE_VERSION)

    val expected =
      DataplaneInitResponse(
        dataplaneName = dataplane.name,
        dataplaneId = dataplane.id,
        dataplaneEnabled = dataplane.enabled,
        dataplaneGroupName = dataplaneGroup.name,
        dataplaneGroupId = dataplaneGroup.id,
        organizationId = dataplaneGroup.organizationId,
      )

    Assertions.assertEquals(expected, result)
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "true,true,true",
      "true,false,false",
      "false,true,false",
      "false,false,false",
    ],
  )
  fun `initialize returns enabled if both dataplane and dataplane groups are enabled`(
    dataplaneEnabled: Boolean,
    dataplaneGroupEnabled: Boolean,
    expected: Boolean,
  ) {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId, enabled = dataplaneEnabled)
    val dataplaneGroup = createDataplaneGroup(dataplane.dataplaneGroupId, enabled = dataplaneGroupEnabled)

    every { dataplaneService.getDataplaneByServiceAccountId(clientId) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId) } returns dataplaneGroup

    val req = DataplaneInitRequestBody(clientId = clientId)

    val result = dataplaneController.initializeDataplane(req, TEST_DATAPLANE_VERSION)

    Assertions.assertEquals(result.dataplaneEnabled, expected)
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "true,true,true",
      "true,false,false",
      "false,true,false",
      "false,false,false",
    ],
  )
  fun `heartbeat returns enabled if both dataplane and dataplane groups are enabled`(
    dataplaneEnabled: Boolean,
    dataplaneGroupEnabled: Boolean,
    expected: Boolean,
  ) {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId, enabled = dataplaneEnabled)
    val dataplaneGroup = createDataplaneGroup(dataplane.dataplaneGroupId, enabled = dataplaneGroupEnabled)

    every { dataplaneService.getDataplaneByServiceAccountId(clientId) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId) } returns dataplaneGroup

    val req = DataplaneHeartbeatRequestBody(clientId = clientId)

    val result = dataplaneController.heartbeatDataplane(req, TEST_DATAPLANE_VERSION)

    Assertions.assertEquals(result.dataplaneEnabled, expected)

    verify {
      dataplaneHealthService.recordHeartbeat(
        dataplaneId = dataplaneId,
        controlPlaneVersion = TEST_CONTROL_PLANE_VERSION,
        dataplaneVersion = TEST_DATAPLANE_VERSION,
      )
    }
  }

  @Test
  fun `heartbeat records no diagnostics when the data plane sends none`() {
    val (_, _, recorded) = heartbeatWithDiagnostics(null)

    Assertions.assertEquals(emptyList<DataplaneDiagnostic>(), recorded)
  }

  @Test
  fun `heartbeat records each diagnostic for the data plane's organization`() {
    val passed = secretStorageResult(DataplaneDiagnosticStatus.PASSED)
    val failed = secretStorageResult(DataplaneDiagnosticStatus.FAILED, errorCode = "ACCESS_DENIED", errorMessage = "Access denied")
    val unableToComplete =
      secretStorageResult(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, errorCode = "TIMEOUT", errorMessage = "Timed out")

    val (_, _, recorded) = heartbeatWithDiagnostics(listOf(passed, failed, unableToComplete))

    val expected =
      listOf(
        expectedDiagnostic(passed, DataplaneDiagnosticOutcome.Passed),
        expectedDiagnostic(failed, DataplaneDiagnosticOutcome.Failed("ACCESS_DENIED", "Access denied")),
        expectedDiagnostic(unableToComplete, DataplaneDiagnosticOutcome.UnableToComplete("TIMEOUT", "Timed out")),
      )
    Assertions.assertEquals(expected, recorded)
  }

  @Test
  fun `heartbeat skips invalid diagnostics and still records the check-in and valid ones`() {
    val valid = secretStorageResult(DataplaneDiagnosticStatus.PASSED)
    val failedWithoutMessage = secretStorageResult(DataplaneDiagnosticStatus.FAILED, errorCode = "ACCESS_DENIED")
    val unableToCompleteWithoutCode = secretStorageResult(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, errorMessage = "Timed out")
    val failedWithBlankCode = secretStorageResult(DataplaneDiagnosticStatus.FAILED, errorCode = "", errorMessage = "Access denied")
    val missingScopeId = secretStorageResult(DataplaneDiagnosticStatus.PASSED).copy(observation = mapOf("scope_type" to "workspace"))
    val missingObservation = secretStorageResult(DataplaneDiagnosticStatus.PASSED).copy(observation = null)
    val errorCodeTooLong = secretStorageResult(DataplaneDiagnosticStatus.FAILED, errorCode = "E".repeat(257), errorMessage = "Access denied")
    val errorCodeAtLimit = secretStorageResult(DataplaneDiagnosticStatus.FAILED, errorCode = "E".repeat(256), errorMessage = "Access denied")

    val (_, dataplaneId, recorded) =
      heartbeatWithDiagnostics(
        listOf(
          valid,
          errorCodeAtLimit,
          failedWithoutMessage,
          unableToCompleteWithoutCode,
          failedWithBlankCode,
          missingScopeId,
          missingObservation,
          errorCodeTooLong,
        ),
      )

    val expected =
      listOf(
        expectedDiagnostic(valid, DataplaneDiagnosticOutcome.Passed),
        expectedDiagnostic(errorCodeAtLimit, DataplaneDiagnosticOutcome.Failed("E".repeat(256), "Access denied")),
      )
    Assertions.assertEquals(expected, recorded)
    verify {
      dataplaneHealthService.recordHeartbeat(
        dataplaneId = dataplaneId,
        controlPlaneVersion = TEST_CONTROL_PLANE_VERSION,
        dataplaneVersion = TEST_DATAPLANE_VERSION,
      )
    }
  }

  private data class RecordedDiagnostics(
    val organizationId: UUID,
    val dataplaneId: UUID,
    val diagnostics: List<DataplaneDiagnostic>,
  )

  private fun heartbeatWithDiagnostics(diagnostics: List<DataplaneDiagnosticResult>?): RecordedDiagnostics {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)
    val dataplaneGroup = createDataplaneGroup(dataplane.dataplaneGroupId, organizationId = organizationId)
    every { dataplaneService.getDataplaneByServiceAccountId(clientId) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId) } returns dataplaneGroup
    val recorded = slot<List<DataplaneDiagnostic>>()
    every { dataplaneHealthService.recordDiagnostics(dataplaneId, organizationId, capture(recorded)) } just Runs

    dataplaneController.heartbeatDataplane(DataplaneHeartbeatRequestBody(clientId = clientId, diagnostics = diagnostics), TEST_DATAPLANE_VERSION)

    return RecordedDiagnostics(organizationId, dataplaneId, recorded.captured)
  }

  private fun secretStorageResult(
    status: DataplaneDiagnosticStatus,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = ApiDataplaneDiagnosticKind.SECRET_STORAGE,
    targetId = UUID.randomUUID(),
    status = status,
    checkedAt = CHECKED_AT.toInstant().toEpochMilli(),
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = mapOf("scope_type" to "workspace", "scope_id" to UUID.randomUUID().toString()),
  )

  private fun expectedDiagnostic(
    result: DataplaneDiagnosticResult,
    outcome: DataplaneDiagnosticOutcome,
  ) = DataplaneDiagnostic(
    kind = DataplaneDiagnosticKind.SECRET_STORAGE,
    targetId = result.targetId,
    outcome = outcome,
    checkedAt = CHECKED_AT,
    observation = result.observation.orEmpty(),
  )

  private fun createDataplane(
    id: UUID? = null,
    enabled: Boolean = false,
  ): Dataplane =
    io.airbyte.data.repositories.entities
      .Dataplane(
        id = id ?: UUID.randomUUID(),
        dataplaneGroupId = MOCK_DATAPLANE_GROUP_ID,
        name = "Test Dataplane",
        enabled = enabled,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        tombstone = false,
      ).toConfigModel()

  private fun createDataplaneGroup(
    id: UUID? = null,
    enabled: Boolean = false,
    organizationId: UUID = UUID.randomUUID(),
  ): DataplaneGroup =
    io.airbyte.data.repositories.entities
      .DataplaneGroup(
        id = id ?: UUID.randomUUID(),
        organizationId = organizationId,
        name = "Test Dataplane Group",
        enabled = enabled,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        tombstone = false,
      ).toConfigModel()

  @Test
  fun `listDataplaneHealth returns health status for multiple dataplanes`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()
    val dataplaneId3 = UUID.randomUUID()
    val mockOrganizationId = UUID.randomUUID()

    val dataplane1 = createDataplane(dataplaneId1, enabled = true)
    val dataplane2 = createDataplane(dataplaneId2, enabled = true)
    val dataplane3 = createDataplane(dataplaneId3, enabled = true)

    val dataplaneGroupEntity =
      io.airbyte.data.repositories.entities
        .DataplaneGroup(
          id = MOCK_DATAPLANE_GROUP_ID,
          organizationId = mockOrganizationId,
          name = "Test Dataplane Group",
          enabled = true,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
          tombstone = false,
        )
    val dataplaneGroup = dataplaneGroupEntity.toConfigModel()

    val now = OffsetDateTime.now()

    val healthStatus1 =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId1,
        status = DataplaneHealthInfo.HealthStatus.HEALTHY,
        lastHeartbeatTimestamp = now.minusSeconds(30),
        secondsSinceLastHeartbeat = 30,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
        healthHistory = emptyList(),
      )

    val healthStatus2 =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId2,
        status = DataplaneHealthInfo.HealthStatus.DEGRADED,
        lastHeartbeatTimestamp = now.minusMinutes(2),
        secondsSinceLastHeartbeat = 120,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = "1.0.1",
        dataplaneVersion = "2.0.1",
        healthHistory = emptyList(),
      )

    val healthStatus3 =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId3,
        status = DataplaneHealthInfo.HealthStatus.UNHEALTHY,
        lastHeartbeatTimestamp = now.minusMinutes(10),
        secondsSinceLastHeartbeat = 600,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = "1.0.2",
        dataplaneVersion = "2.0.2",
        healthHistory = emptyList(),
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane1, dataplane2, dataplane3)

    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId1, dataplaneId2, dataplaneId3), false) } returns
      listOf(healthStatus1, healthStatus2, healthStatus3)

    val request = OrganizationIdRequestBody(organizationId = mockOrganizationId)
    val response = dataplaneController.listDataplaneHealth(request)

    Assertions.assertEquals(3, response.dataplanes.size)

    val health1 = response.dataplanes[0]
    Assertions.assertEquals(dataplaneId1, health1.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.Status.HEALTHY, health1.status)
    Assertions.assertEquals("1.0.0", health1.controlPlaneVersion)
    Assertions.assertEquals("2.0.0", health1.dataplaneVersion)

    val health2 = response.dataplanes[1]
    Assertions.assertEquals(dataplaneId2, health2.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.Status.DEGRADED, health2.status)

    val health3 = response.dataplanes[2]
    Assertions.assertEquals(dataplaneId3, health3.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.Status.UNHEALTHY, health3.status)

    response.dataplanes.forEach { health ->
      Assertions.assertEquals(true, health.enabled)
      Assertions.assertNull(health.healthHistory)
    }
    verify(exactly = 1) { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) }
    verify(exactly = 1) { featureFlagClient.boolVariation(DataplaneHealthUi, Organization(mockOrganizationId)) }
  }

  @Test
  fun `listDataplaneHealth returns the history when the data plane health UI flag is on`() {
    val dataplaneId = UUID.randomUUID()
    val mockOrganizationId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId, enabled = true)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = true, organizationId = mockOrganizationId)
    val bucketStart = OffsetDateTime.now().minusHours(24)
    val healthInfo =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId,
        status = DataplaneHealthInfo.HealthStatus.HEALTHY,
        lastHeartbeatTimestamp = OffsetDateTime.now().minusSeconds(30),
        secondsSinceLastHeartbeat = 30,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
        healthHistory =
          listOf(
            HealthHistoryBucket(bucketStart = bucketStart, status = DataplaneHealthInfo.HealthStatus.UNKNOWN),
            HealthHistoryBucket(bucketStart = bucketStart.plusMinutes(15), status = DataplaneHealthInfo.HealthStatus.DEGRADED),
          ),
      )

    every { featureFlagClient.boolVariation(DataplaneHealthUi, Organization(mockOrganizationId)) } returns true
    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), true) } returns listOf(healthInfo)

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    val health = response.dataplanes.single()
    Assertions.assertEquals(true, health.enabled)
    Assertions.assertEquals(
      listOf(
        DataplaneHealthHistoryBucket(bucketStart = bucketStart.toInstant().toEpochMilli(), status = DataplaneHealthHistoryBucket.Status.UNKNOWN),
        DataplaneHealthHistoryBucket(
          bucketStart = bucketStart.plusMinutes(15).toInstant().toEpochMilli(),
          status = DataplaneHealthHistoryBucket.Status.DEGRADED,
        ),
      ),
      health.healthHistory,
    )
  }

  @Test
  fun `listDataplaneHealth reports enabled false when the dataplane group is disabled`() {
    val dataplaneId = UUID.randomUUID()
    val mockOrganizationId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId, enabled = true)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = false, organizationId = mockOrganizationId)
    val healthInfo =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId,
        status = DataplaneHealthInfo.HealthStatus.UNKNOWN,
        lastHeartbeatTimestamp = null,
        secondsSinceLastHeartbeat = null,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = null,
        dataplaneVersion = null,
        healthHistory = emptyList(),
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), false) } returns listOf(healthInfo)

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    Assertions.assertEquals(false, response.dataplanes.single().enabled)
  }

  @Test
  fun `listDataplaneHealth reports enabled false when the dataplane is disabled`() {
    val dataplaneId = UUID.randomUUID()
    val mockOrganizationId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId, enabled = false)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = true, organizationId = mockOrganizationId)
    val healthInfo =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId,
        status = DataplaneHealthInfo.HealthStatus.UNKNOWN,
        lastHeartbeatTimestamp = null,
        secondsSinceLastHeartbeat = null,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = null,
        dataplaneVersion = null,
        healthHistory = emptyList(),
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), false) } returns listOf(healthInfo)

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    Assertions.assertEquals(false, response.dataplanes.single().enabled)
  }

  @Test
  fun `listDataplaneHealth handles dataplane with UNKNOWN status`() {
    val dataplaneId = UUID.randomUUID()
    val mockOrganizationId = UUID.randomUUID()

    val dataplane = createDataplane(dataplaneId)

    val dataplaneGroupEntity =
      io.airbyte.data.repositories.entities
        .DataplaneGroup(
          id = MOCK_DATAPLANE_GROUP_ID,
          organizationId = mockOrganizationId,
          name = "Test Dataplane Group",
          enabled = true,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
          tombstone = false,
        )
    val dataplaneGroup = dataplaneGroupEntity.toConfigModel()

    val healthStatus =
      DataplaneHealthInfo(
        dataplaneId = dataplaneId,
        status = DataplaneHealthInfo.HealthStatus.UNKNOWN,
        lastHeartbeatTimestamp = null,
        secondsSinceLastHeartbeat = null,
        recentHeartbeats = emptyList(),
        controlPlaneVersion = null,
        dataplaneVersion = null,
        healthHistory = emptyList(),
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), false) } returns listOf(healthStatus)

    val request = OrganizationIdRequestBody(organizationId = mockOrganizationId)
    val response = dataplaneController.listDataplaneHealth(request)

    Assertions.assertEquals(1, response.dataplanes.size)

    val health = response.dataplanes[0]
    Assertions.assertEquals(dataplaneId, health.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.Status.UNKNOWN, health.status)
    Assertions.assertNull(health.lastHeartbeatTimestamp)
    Assertions.assertNull(health.controlPlaneVersion)
    Assertions.assertNull(health.dataplaneVersion)
  }

  @Test
  fun `listDataplaneHealth returns each data plane's saved diagnostics`() {
    val mockOrganizationId = UUID.randomUUID()
    val dataplaneWithResults = createDataplane(UUID.randomUUID(), enabled = true)
    val dataplaneWithoutResults = createDataplane(UUID.randomUUID(), enabled = true)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = true, organizationId = mockOrganizationId)
    val dataplaneIds = listOf(dataplaneWithResults.id, dataplaneWithoutResults.id)
    val updatedAt = CHECKED_AT.plusSeconds(30)
    val observation = mapOf("scope_type" to "workspace", "scope_id" to UUID.randomUUID().toString())
    val passed =
      DataplaneDiagnostic(DataplaneDiagnosticKind.SECRET_STORAGE, UUID.randomUUID(), DataplaneDiagnosticOutcome.Passed, CHECKED_AT, observation)
    val failed =
      DataplaneDiagnostic(
        DataplaneDiagnosticKind.SECRET_STORAGE,
        UUID.randomUUID(),
        DataplaneDiagnosticOutcome.Failed("ACCESS_DENIED", "Access denied"),
        CHECKED_AT,
        emptyMap(),
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplaneWithResults, dataplaneWithoutResults)
    every { dataplaneHealthService.getDataplaneHealthInfos(dataplaneIds, false) } returns dataplaneIds.map { unknownHealthInfo(it) }
    every { dataplaneHealthService.getDiagnostics(mockOrganizationId, dataplaneIds) } returns
      listOf(
        SavedDataplaneDiagnostic(dataplaneWithResults.id, passed, updatedAt),
        SavedDataplaneDiagnostic(dataplaneWithResults.id, failed, updatedAt),
      )

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    val expected =
      mapOf(
        dataplaneWithResults.id to
          listOf(
            DataplaneDiagnosticRead(
              kind = ApiDataplaneDiagnosticKind.SECRET_STORAGE,
              targetId = passed.targetId,
              status = DataplaneDiagnosticStatus.PASSED,
              checkedAt = CHECKED_AT.toInstant().toEpochMilli(),
              updatedAt = updatedAt.toInstant().toEpochMilli(),
              observation = observation,
            ),
            DataplaneDiagnosticRead(
              kind = ApiDataplaneDiagnosticKind.SECRET_STORAGE,
              targetId = failed.targetId,
              status = DataplaneDiagnosticStatus.FAILED,
              checkedAt = CHECKED_AT.toInstant().toEpochMilli(),
              updatedAt = updatedAt.toInstant().toEpochMilli(),
              errorCode = "ACCESS_DENIED",
              errorMessage = "Access denied",
              observation = emptyMap(),
            ),
          ),
        dataplaneWithoutResults.id to emptyList(),
      )
    Assertions.assertEquals(expected, response.dataplanes.associate { it.dataplaneId to it.diagnostics })
  }

  @Test
  fun `listDataplaneHealth returns the checks each data plane is too old to run`() {
    val mockOrganizationId = UUID.randomUUID()
    val oldDataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val newDataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = true, organizationId = mockOrganizationId)
    val dataplaneIds = listOf(oldDataplane.id, newDataplane.id)

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(oldDataplane, newDataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(dataplaneIds, false) } returns
      listOf(
        unknownHealthInfo(oldDataplane.id).copy(dataplaneVersion = "2.4.0"),
        unknownHealthInfo(newDataplane.id).copy(dataplaneVersion = "2.6.0"),
      )
    every { dataplaneHealthService.getDiagnostics(mockOrganizationId, dataplaneIds) } returns emptyList()
    every { dataplaneHealthService.unsupportedDiagnosticKinds("2.4.0", emptyList()) } returns listOf(DataplaneDiagnosticKind.SECRET_STORAGE)
    every { dataplaneHealthService.unsupportedDiagnosticKinds("2.6.0", emptyList()) } returns emptyList()

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    val expected =
      mapOf(
        oldDataplane.id to listOf(ApiDataplaneDiagnosticKind.SECRET_STORAGE),
        newDataplane.id to emptyList(),
      )
    Assertions.assertEquals(expected, response.dataplanes.associate { it.dataplaneId to it.unsupportedDiagnosticKinds })
  }

  @Test
  fun `listDataplaneHealth returns each data plane's version support next to its unchanged check-in status when the health UI flag is on`() {
    val mockOrganizationId = UUID.randomUUID()
    val supportedDataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val endedDataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val customDataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val silentDataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = true, organizationId = mockOrganizationId)
    val dataplanes = listOf(supportedDataplane, endedDataplane, customDataplane, silentDataplane)
    val supportedRelease =
      DataplaneVersionSupport.Published(
        publishedAt = OffsetDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        supportedUntil = OffsetDateTime.of(2027, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        supported = true,
      )
    val endedRelease =
      DataplaneVersionSupport.Published(
        publishedAt = OffsetDateTime.of(2025, 1, 15, 0, 0, 0, 0, ZoneOffset.UTC),
        supportedUntil = OffsetDateTime.of(2026, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC),
        supported = false,
      )

    every { featureFlagClient.boolVariation(DataplaneHealthUi, Organization(mockOrganizationId)) } returns true
    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns dataplanes
    every { dataplaneHealthService.getDataplaneHealthInfos(dataplanes.map { it.id }, true) } returns
      listOf(
        unknownHealthInfo(supportedDataplane.id).copy(status = DataplaneHealthInfo.HealthStatus.UNHEALTHY, dataplaneVersion = "2.6.0"),
        unknownHealthInfo(endedDataplane.id).copy(status = DataplaneHealthInfo.HealthStatus.HEALTHY, dataplaneVersion = "2.0.0"),
        unknownHealthInfo(customDataplane.id).copy(status = DataplaneHealthInfo.HealthStatus.HEALTHY, dataplaneVersion = "2.6.0-custom"),
        unknownHealthInfo(silentDataplane.id),
      )
    every { dataplaneHealthService.getPublishedReleaseSupport(setOf("2.6.0", "2.0.0", "2.6.0-custom"), any()) } returns
      mapOf("2.6.0" to supportedRelease, "2.0.0" to endedRelease)

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    val expected =
      mapOf(
        supportedDataplane.id to
          Pair(
            DataplaneHealthRead.Status.UNHEALTHY,
            ApiDataplaneVersionSupport(
              status = ApiDataplaneVersionSupport.Status.SUPPORTED,
              publishedAt = supportedRelease.publishedAt.toInstant().toEpochMilli(),
              supportedUntil = supportedRelease.supportedUntil.toInstant().toEpochMilli(),
            ),
          ),
        endedDataplane.id to
          Pair(
            DataplaneHealthRead.Status.HEALTHY,
            ApiDataplaneVersionSupport(
              status = ApiDataplaneVersionSupport.Status.SUPPORT_ENDED,
              publishedAt = endedRelease.publishedAt.toInstant().toEpochMilli(),
              supportedUntil = endedRelease.supportedUntil.toInstant().toEpochMilli(),
            ),
          ),
        customDataplane.id to
          Pair(
            DataplaneHealthRead.Status.HEALTHY,
            ApiDataplaneVersionSupport(status = ApiDataplaneVersionSupport.Status.UNKNOWN),
          ),
        silentDataplane.id to
          Pair(
            DataplaneHealthRead.Status.UNKNOWN,
            ApiDataplaneVersionSupport(status = ApiDataplaneVersionSupport.Status.UNKNOWN),
          ),
      )
    Assertions.assertEquals(expected, response.dataplanes.associate { it.dataplaneId to Pair(it.status, it.dataplaneVersionSupport) })
  }

  @Test
  fun `listDataplaneHealth leaves out version support and skips the release catalog when the health UI flag is off`() {
    val mockOrganizationId = UUID.randomUUID()
    val dataplane = createDataplane(UUID.randomUUID(), enabled = true)
    val dataplaneGroup = createDataplaneGroup(MOCK_DATAPLANE_GROUP_ID, enabled = true, organizationId = mockOrganizationId)

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplane.id), false) } returns
      listOf(unknownHealthInfo(dataplane.id).copy(status = DataplaneHealthInfo.HealthStatus.HEALTHY, dataplaneVersion = "2.7.0-flag-off"))

    val response = dataplaneController.listDataplaneHealth(OrganizationIdRequestBody(organizationId = mockOrganizationId))

    Assertions.assertEquals(
      mapOf(dataplane.id to Pair(DataplaneHealthRead.Status.HEALTHY, null)),
      response.dataplanes.associate { it.dataplaneId to Pair(it.status, it.dataplaneVersionSupport) },
    )
    verify(exactly = 0) { dataplaneHealthService.getPublishedReleaseSupport(setOf("2.7.0-flag-off"), any()) }
  }

  private fun unknownHealthInfo(dataplaneId: UUID) =
    DataplaneHealthInfo(
      dataplaneId = dataplaneId,
      status = DataplaneHealthInfo.HealthStatus.UNKNOWN,
      lastHeartbeatTimestamp = null,
      secondsSinceLastHeartbeat = null,
      recentHeartbeats = emptyList(),
      controlPlaneVersion = null,
      dataplaneVersion = null,
      healthHistory = emptyList(),
    )

  @Test
  fun `listDataplaneHealth handles empty organization`() {
    val mockOrganizationId = UUID.randomUUID()

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns emptyList()
    every { dataplaneService.listDataplanes(emptyList<UUID>()) } returns emptyList()

    val request = OrganizationIdRequestBody(organizationId = mockOrganizationId)
    val response = dataplaneController.listDataplaneHealth(request)

    Assertions.assertEquals(0, response.dataplanes.size)
  }
}
