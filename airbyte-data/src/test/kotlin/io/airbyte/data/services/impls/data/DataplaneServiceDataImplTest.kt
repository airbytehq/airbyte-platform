/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.repositories.entities.Dataplane
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.airbyte.domain.models.ServiceAccount
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

private val MOCK_DATAPLANE_GROUP_ID = UUID.randomUUID()
private const val MOCK_DATAPLANE_NAME = "Test Dataplane"
private const val MOCK_SERVICE_ACCOUNT_SECRET = "secret"

class DataplaneServiceDataImplTest {
  private val dataplaneRepository = mockk<DataplaneRepository>()
  private val dataplaneGroupRepository = mockk<DataplaneGroupRepository>()
  private val serviceAccountsService = mockk<ServiceAccountsService>()
  private val permissionService = mockk<PermissionService>()
  private var dataplaneServiceDataImpl =
    DataplaneServiceDataImpl(dataplaneRepository, dataplaneGroupRepository, serviceAccountsService, permissionService)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get dataplane by id`() {
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)

    every { dataplaneRepository.findById(dataplaneId) } returns Optional.of(dataplane)

    val retrievedDataplane = dataplaneServiceDataImpl.getDataplane(dataplaneId)
    assertEquals(dataplane.toConfigModel(), retrievedDataplane)

    verify { dataplaneRepository.findById(dataplaneId) }
  }

  @Test
  fun `test get dataplane by non-existent id throws`() {
    every { dataplaneRepository.findById(any()) } returns Optional.empty()

    assertThrows<ConfigNotFoundException> { dataplaneServiceDataImpl.getDataplane(UUID.randomUUID()) }

    verify { dataplaneRepository.findById(any()) }
  }

  @Test
  fun `test create new dataplane`() {
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)
    val dataplaneWithServiceAccount = createDataplaneWithServiceAccount(dataplaneId)
    val serviceAccount = createServiceAccount(dataplaneId)
    val group = DataplaneGroup(organizationId = UUID.randomUUID(), name = "org", enabled = true, tombstone = false)

    every { dataplaneRepository.existsById(dataplane.id) } returns false
    every { dataplaneGroupRepository.findById(dataplane.dataplaneGroupId) } returns Optional.of(group)
    every { dataplaneRepository.save(any()) } returns dataplane
    every { serviceAccountsService.create(any(), any(), any()) } returns serviceAccount
    every { permissionService.createServiceAccountPermission(any()) } returns mockk<Permission>()

    val retrievedDataplane = dataplaneServiceDataImpl.createDataplaneAndServiceAccount(dataplane.toConfigModel())
    assertEquals(dataplaneWithServiceAccount, retrievedDataplane)

    verify { dataplaneRepository.existsById(dataplane.id) }
    verify { dataplaneRepository.save(any()) }
  }

  @Test
  fun `test update existing dataplane`() {
    val dataplane = createDataplane(UUID.randomUUID())

    every { dataplaneRepository.existsById(dataplane.id) } returns true
    every { dataplaneRepository.update(any()) } returns dataplane

    val retrievedDataplane = dataplaneServiceDataImpl.updateDataplane(dataplane.toConfigModel())
    assertEquals(dataplane.toConfigModel(), retrievedDataplane)

    verify { dataplaneRepository.existsById(dataplane.id) }
    verify { dataplaneRepository.update(any()) }
  }

  @Test
  fun `test list dataplanes by dataplane group id with tombstones`() {
    val mockDataplaneGroupId = UUID.randomUUID()
    val mockDataplaneId1 = UUID.randomUUID()
    val mockDataplaneId2 = UUID.randomUUID()
    every { dataplaneRepository.findAllByDataplaneGroupIdOrderByUpdatedAtDesc(mockDataplaneGroupId) } returns
      listOf(
        createDataplane(mockDataplaneId1),
        createDataplane(mockDataplaneId2),
      )

    val retrievedDataplanes = dataplaneServiceDataImpl.listDataplanes(mockDataplaneGroupId, true)

    assertEquals(retrievedDataplanes.size, 2)
  }

  @Test
  fun `test list dataplanes by dataplane group id without tombstones`() {
    val mockDataplaneGroupId = UUID.randomUUID()
    val mockDataplaneId1 = UUID.randomUUID()
    val mockDataplaneId2 = UUID.randomUUID()
    every { dataplaneRepository.findAllByDataplaneGroupIdAndTombstoneFalseOrderByUpdatedAtDesc(mockDataplaneGroupId) } returns
      listOf(
        createDataplane(mockDataplaneId1),
        createDataplane(mockDataplaneId2),
      )

    val retrievedDataplanes = dataplaneServiceDataImpl.listDataplanes(mockDataplaneGroupId, false)

    assertEquals(retrievedDataplanes.size, 2)
  }

  private fun createDataplane(id: UUID): Dataplane =
    Dataplane(
      id = id,
      dataplaneGroupId = MOCK_DATAPLANE_GROUP_ID,
      name = MOCK_DATAPLANE_NAME,
      enabled = true,
      tombstone = false,
      createdAt = OffsetDateTime.now(),
      updatedAt = OffsetDateTime.now(),
    )

  private fun createDataplaneWithServiceAccount(id: UUID): DataplaneWithServiceAccount =
    DataplaneWithServiceAccount(
      dataplane = createDataplane(id).toConfigModel(),
      serviceAccount = createServiceAccount(id),
    )

  private fun createServiceAccount(dataplaneId: UUID): ServiceAccount =
    ServiceAccount(
      id = dataplaneId,
      name = "service-account-name",
      managed = true,
      secret = MOCK_SERVICE_ACCOUNT_SECRET,
    )
}
