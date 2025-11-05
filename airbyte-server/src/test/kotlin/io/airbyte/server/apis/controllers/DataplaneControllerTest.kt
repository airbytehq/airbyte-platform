/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataplaneDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneHealthRead
import io.airbyte.api.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.model.generated.DataplaneHeartbeatResponse
import io.airbyte.api.model.generated.DataplaneInitRequestBody
import io.airbyte.api.model.generated.DataplaneInitResponse
import io.airbyte.api.model.generated.DataplaneListRequestBody
import io.airbyte.api.model.generated.DataplaneTokenRequestBody
import io.airbyte.api.model.generated.DataplaneUpdateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.airbyte.domain.models.DataplaneHealthInfo
import io.airbyte.domain.models.OrganizationId
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.time.OffsetDateTime
import java.util.UUID
import io.airbyte.server.services.DataplaneService as ServerDataplaneService

class DataplaneControllerTest {
  companion object {
    private const val TEST_DATAPLANE_VERSION = "test-123"
    private const val TEST_CONTROL_PLANE_VERSION = "1.0.0"

    private val dataplaneService = mockk<ServerDataplaneService>()
    private val dataplaneGroupService = mockk<DataplaneGroupService>()
    private val dataplaneHealthService = mockk<DataplaneHealthService>(relaxed = true)
    private val roleResolver = mockk<RoleResolver>(relaxed = true)
    private val entitlementService = mockk<EntitlementService>(relaxed = true)
    private val airbyteConfig =
      mockk<AirbyteConfig>().apply {
        every { version } returns TEST_CONTROL_PLANE_VERSION
      }
    private val dataplaneController =
      DataplaneController(
        dataplaneService,
        dataplaneGroupService,
        dataplaneHealthService,
        roleResolver,
        entitlementService,
        mockk(relaxed = true),
        airbyteConfig,
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
        DataplaneUpdateRequestBody()
          .dataplaneId(mockDataplane.id)
          .name(newName)
          .enabled(newEnabled),
      )

    assert(updatedDataplane.dataplaneId == mockDataplane.id)
    assert(updatedDataplane.name == newName)
    assert(updatedDataplane.enabled == newEnabled)
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
      DataplaneDeleteRequestBody().dataplaneId(mockDataplane.id)

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
    val dataplanes = dataplaneController.listDataplanes(DataplaneListRequestBody().dataplaneGroupId(MOCK_DATAPLANE_GROUP_ID))
    val responseDataplanes = dataplanes.dataplanes

    assert(responseDataplanes.size == 2)
    assert(responseDataplanes[0].dataplaneId == dataplaneId1)
    assert(responseDataplanes[1].dataplaneId == dataplaneId2)
  }

  @Test
  fun `getDataplaneToken returns a valid token`() {
    val clientId = "test-client-id"
    val clientSecret = "test-client-secret"
    val expectedToken = "test-token"

    every { dataplaneService.getToken(clientId, clientSecret) } returns expectedToken

    val requestBody = DataplaneTokenRequestBody().clientId(clientId).clientSecret(clientSecret)
    val accessToken = dataplaneController.getDataplaneToken(requestBody)

    assert(accessToken.accessToken == expectedToken)
  }

  @Test
  fun `heartbeatDataplane returns expected dataplane information`() {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)
    val dataplaneGroup = createDataplaneGroup(dataplane.dataplaneGroupId)

    every { dataplaneService.getDataplaneByServiceAccountId(clientId) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(dataplane.dataplaneGroupId) } returns dataplaneGroup

    val req = DataplaneHeartbeatRequestBody()
    req.clientId = clientId

    val result = dataplaneController.heartbeatDataplane(req, TEST_DATAPLANE_VERSION)

    val expected = DataplaneHeartbeatResponse()
    expected.dataplaneName = dataplane.name
    expected.dataplaneId = dataplane.id
    expected.dataplaneEnabled = dataplane.enabled
    expected.dataplaneGroupName = dataplaneGroup.name
    expected.dataplaneGroupId = dataplaneGroup.id
    expected.organizationId = dataplaneGroup.organizationId

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

    val req = DataplaneInitRequestBody()
    req.clientId = clientId

    val result = dataplaneController.initializeDataplane(req, TEST_DATAPLANE_VERSION)

    val expected = DataplaneInitResponse()
    expected.dataplaneName = dataplane.name
    expected.dataplaneId = dataplane.id
    expected.dataplaneEnabled = dataplane.enabled
    expected.dataplaneGroupName = dataplaneGroup.name
    expected.dataplaneGroupId = dataplaneGroup.id
    expected.organizationId = dataplaneGroup.organizationId

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

    val req = DataplaneInitRequestBody()
    req.clientId = clientId

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

    val req = DataplaneHeartbeatRequestBody()
    req.clientId = clientId

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
  ): DataplaneGroup =
    io.airbyte.data.repositories.entities
      .DataplaneGroup(
        id = id ?: UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
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

    val dataplane1 = createDataplane(dataplaneId1)
    val dataplane2 = createDataplane(dataplaneId2)
    val dataplane3 = createDataplane(dataplaneId3)

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
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane1, dataplane2, dataplane3)

    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId1, dataplaneId2, dataplaneId3)) } returns
      listOf(healthStatus1, healthStatus2, healthStatus3)

    val request = OrganizationIdRequestBody().organizationId(mockOrganizationId)
    val response = dataplaneController.listDataplaneHealth(request)

    Assertions.assertEquals(3, response.dataplanes.size)

    val health1 = response.dataplanes[0]
    Assertions.assertEquals(dataplaneId1, health1.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.StatusEnum.HEALTHY, health1.status)
    Assertions.assertEquals("1.0.0", health1.controlPlaneVersion)
    Assertions.assertEquals("2.0.0", health1.dataplaneVersion)

    val health2 = response.dataplanes[1]
    Assertions.assertEquals(dataplaneId2, health2.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.StatusEnum.DEGRADED, health2.status)

    val health3 = response.dataplanes[2]
    Assertions.assertEquals(dataplaneId3, health3.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.StatusEnum.UNHEALTHY, health3.status)

    verify(exactly = 1) { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) }
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
      )

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns listOf(dataplaneGroup)
    every { dataplaneService.listDataplanes(listOf(MOCK_DATAPLANE_GROUP_ID)) } returns listOf(dataplane)
    every { dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId)) } returns listOf(healthStatus)

    val request = OrganizationIdRequestBody().organizationId(mockOrganizationId)
    val response = dataplaneController.listDataplaneHealth(request)

    Assertions.assertEquals(1, response.dataplanes.size)

    val health = response.dataplanes[0]
    Assertions.assertEquals(dataplaneId, health.dataplaneId)
    Assertions.assertEquals(DataplaneHealthRead.StatusEnum.UNKNOWN, health.status)
    Assertions.assertNull(health.lastHeartbeatTimestamp)
    Assertions.assertNull(health.controlPlaneVersion)
    Assertions.assertNull(health.dataplaneVersion)
  }

  @Test
  fun `listDataplaneHealth handles empty organization`() {
    val mockOrganizationId = UUID.randomUUID()

    every { entitlementService.ensureEntitled(OrganizationId(mockOrganizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.listDataplaneGroups(listOf(mockOrganizationId), false) } returns emptyList()
    every { dataplaneService.listDataplanes(emptyList<UUID>()) } returns emptyList()

    val request = OrganizationIdRequestBody().organizationId(mockOrganizationId)
    val response = dataplaneController.listDataplaneHealth(request)

    Assertions.assertEquals(0, response.dataplanes.size)
  }
}
