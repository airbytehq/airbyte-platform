/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataplaneDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneListRequestBody
import io.airbyte.api.model.generated.DataplaneTokenRequestBody
import io.airbyte.api.model.generated.DataplaneUpdateRequestBody
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Dataplane
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.server.services.DataplaneService as ServerDataplaneService

class DataplaneControllerTest {
  companion object {
    private val dataplaneService = mockk<ServerDataplaneService>()
    private val currentUserService = mockk<CurrentUserService>()
    private val dataplaneController = DataplaneController(dataplaneService, currentUserService)
    private val MOCK_DATAPLANE_GROUP_ID = UUID.randomUUID()
  }

  @Test
  fun `updateDataplane returns the dataplane`() {
    val mockDataplane = createDataplane()
    val newName = "new name"
    val newEnabled = true

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneService.updateDataplane(any(), any(), any(), any()) } returns
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
    val updatedById = UUID.randomUUID()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(updatedById)
    every { dataplaneService.deleteDataplane(any(), any()) } returns mockDataplane

    val dataplaneDeleteRequestBody =
      DataplaneDeleteRequestBody().dataplaneId(mockDataplane.id)

    dataplaneController.deleteDataplane(dataplaneDeleteRequestBody)

    verify { dataplaneService.deleteDataplane(mockDataplane.id, updatedById) }
  }

  @Test
  fun `listDataplanes returns dataplanes`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
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

  private fun createDataplane(id: UUID? = null): Dataplane =
    io.airbyte.data.repositories.entities
      .Dataplane(
        id = id ?: UUID.randomUUID(),
        dataplaneGroupId = MOCK_DATAPLANE_GROUP_ID,
        name = "Test Dataplane",
        enabled = false,
        updatedBy = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        tombstone = false,
      ).toConfigModel()
}
