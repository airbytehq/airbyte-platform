/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataplaneDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.model.generated.DataplaneHeartbeatResponse
import io.airbyte.api.model.generated.DataplaneInitRequestBody
import io.airbyte.api.model.generated.DataplaneInitResponse
import io.airbyte.api.model.generated.DataplaneListRequestBody
import io.airbyte.api.model.generated.DataplaneTokenRequestBody
import io.airbyte.api.model.generated.DataplaneUpdateRequestBody
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
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
    private val dataplaneService = mockk<ServerDataplaneService>()
    private val dataplaneGroupService = mockk<DataplaneGroupService>()
    private val roleResolver = mockk<RoleResolver>(relaxed = true)
    private val dataplaneController =
      DataplaneController(
        dataplaneService,
        dataplaneGroupService,
        roleResolver,
      )
    private val MOCK_DATAPLANE_GROUP_ID = UUID.randomUUID()
  }

  @Test
  fun `updateDataplane returns the dataplane`() {
    val mockDataplane = createDataplane()
    val newName = "new name"
    val newEnabled = true

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

    val result = dataplaneController.heartbeatDataplane(req)

    val expected = DataplaneHeartbeatResponse()
    expected.dataplaneName = dataplane.name
    expected.dataplaneId = dataplane.id
    expected.dataplaneEnabled = dataplane.enabled
    expected.dataplaneGroupName = dataplaneGroup.name
    expected.dataplaneGroupId = dataplaneGroup.id

    Assertions.assertEquals(expected, result)
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

    val result = dataplaneController.initializeDataplane(req)

    val expected = DataplaneInitResponse()
    expected.dataplaneName = dataplane.name
    expected.dataplaneId = dataplane.id
    expected.dataplaneEnabled = dataplane.enabled
    expected.dataplaneGroupName = dataplaneGroup.name
    expected.dataplaneGroupId = dataplaneGroup.id

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

    val result = dataplaneController.initializeDataplane(req)

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

    val result = dataplaneController.heartbeatDataplane(req)

    Assertions.assertEquals(result.dataplaneEnabled, expected)
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
}
