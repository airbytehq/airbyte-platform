/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.data.repositories.DataWorkerAllocatedCapacityRepository
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.entities.DataWorkerAllocatedCapacity
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.dataworker.DataWorkerAllocation
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.util.Optional
import java.util.UUID
import io.airbyte.config.DataplaneGroup as ConfigDataplaneGroup

internal class DataWorkerAllocatedCapacityServiceTest {
  private lateinit var allocatedCapacityRepository: DataWorkerAllocatedCapacityRepository
  private lateinit var dataplaneGroupRepository: DataplaneGroupRepository
  private lateinit var dataplaneGroupService: DataplaneGroupService
  private lateinit var service: DataWorkerAllocatedCapacityService

  private val organizationId = OrganizationId(UUID.randomUUID())

  // Fixed so the ordering tests can rely on which id sorts first.
  private val lowerRegion = DataplaneGroupId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val higherRegion = DataplaneGroupId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
  private val defaultRegion = DataplaneGroupId(UUID.fromString("33333333-3333-3333-3333-333333333333"))

  @BeforeEach
  fun setUp() {
    allocatedCapacityRepository = mockk(relaxed = true)
    dataplaneGroupRepository = mockk()
    dataplaneGroupService = mockk()
    service =
      DataWorkerAllocatedCapacityService(
        allocatedCapacityRepository,
        dataplaneGroupRepository,
        dataplaneGroupService,
        ImmediateAllocationTransactionOperations(),
      )

    givenRegionExists(lowerRegion)
    givenRegionExists(higherRegion)
    givenDefaultRegionIs(defaultRegion)
  }

  @Test
  fun `getAllocations returns one entry per region the organization holds capacity in`() {
    every { allocatedCapacityRepository.findByOrganizationId(organizationId.value) } returns
      listOf(
        allocationRow(lowerRegion, 10.0),
        allocationRow(higherRegion, 2.5),
      )

    val result = service.getAllocations(organizationId)

    assertEquals(organizationId, result.organizationId)
    assertEquals(
      listOf(
        DataWorkerAllocation(lowerRegion, 10.0),
        DataWorkerAllocation(higherRegion, 2.5),
      ),
      result.allocations,
    )
  }

  @Test
  fun `getAllocations totals the capacity across every region`() {
    every { allocatedCapacityRepository.findByOrganizationId(organizationId.value) } returns
      listOf(
        allocationRow(lowerRegion, 10.0),
        allocationRow(higherRegion, 2.5),
      )

    assertEquals(12.5, service.getAllocations(organizationId).totalAllocatedCapacity)
  }

  @Test
  fun `getAllocations reports an organization with no rows as zero rather than an error`() {
    every { allocatedCapacityRepository.findByOrganizationId(organizationId.value) } returns emptyList()

    val result = service.getAllocations(organizationId)

    assertEquals(emptyList<DataWorkerAllocation>(), result.allocations)
    assertEquals(0.0, result.totalAllocatedCapacity)
  }

  @Test
  fun `getAllocation returns the region's capacity`() {
    every {
      allocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId.value, lowerRegion.value)
    } returns allocationRow(lowerRegion, 7.5)

    assertEquals(DataWorkerAllocation(lowerRegion, 7.5), service.getAllocation(organizationId, lowerRegion))
  }

  @Test
  fun `getAllocation returns null when the organization holds no capacity in the region`() {
    every {
      allocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId.value, lowerRegion.value)
    } returns null

    assertNull(service.getAllocation(organizationId, lowerRegion))
  }

  @Test
  fun `reallocateCapacity rejects a move to the same region`() {
    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, lowerRegion, 1.0)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects a zero amount`() {
    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 0.0)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects a negative amount`() {
    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, -1.0)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects a NaN amount`() {
    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, Double.NaN)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects an infinite amount`() {
    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, Double.POSITIVE_INFINITY)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects an amount larger than the capacity column can hold`() {
    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, Float.MAX_VALUE.toDouble() * 2)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity accepts an amount at the capacity column's limit`() {
    givenSourceHasEnough(lowerRegion)

    service.reallocateCapacity(organizationId, lowerRegion, higherRegion, Float.MAX_VALUE.toDouble())

    verify {
      allocatedCapacityRepository.addCapacity(organizationId.value, higherRegion.value, Float.MAX_VALUE.toDouble())
    }
  }

  @Test
  fun `reallocateCapacity rejects a source region that does not exist`() {
    every { dataplaneGroupRepository.findById(lowerRegion.value) } returns Optional.empty()

    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 1.0)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects a destination region that does not exist`() {
    every { dataplaneGroupRepository.findById(higherRegion.value) } returns Optional.empty()

    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 1.0)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity rejects a destination region that has been deleted`() {
    givenRegionExists(higherRegion, tombstone = true)

    assertThrows(BadRequestProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 1.0)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `reallocateCapacity allows moving capacity out of a deleted region`() {
    givenRegionExists(lowerRegion, tombstone = true)
    givenSourceHasEnough(lowerRegion)

    service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 3.0)

    verify { allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, lowerRegion.value, 3.0) }
    verify { allocatedCapacityRepository.addCapacity(organizationId.value, higherRegion.value, 3.0) }
  }

  @Test
  fun `reallocateCapacity subtracts before adding when the source region sorts first`() {
    givenSourceHasEnough(lowerRegion)

    service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 3.0)

    verifyOrder {
      allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, lowerRegion.value, 3.0)
      allocatedCapacityRepository.addCapacity(organizationId.value, higherRegion.value, 3.0)
    }
  }

  @Test
  fun `reallocateCapacity adds before subtracting when the destination region sorts first`() {
    givenSourceHasEnough(higherRegion)

    service.reallocateCapacity(organizationId, higherRegion, lowerRegion, 3.0)

    verifyOrder {
      allocatedCapacityRepository.addCapacity(organizationId.value, lowerRegion.value, 3.0)
      allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, higherRegion.value, 3.0)
    }
  }

  @Test
  fun `reallocateCapacity fails when the source region cannot give up the capacity`() {
    // 0 rows changed covers both "not enough capacity" and "no capacity there at all". The
    // repository cannot tell them apart, and a move cannot proceed from either.
    every {
      allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, lowerRegion.value, 3.0)
    } returns 0

    assertThrows(StateConflictProblem::class.java) {
      service.reallocateCapacity(organizationId, lowerRegion, higherRegion, 3.0)
    }
  }

  @Test
  fun `reallocateCapacity scopes both writes to the organization`() {
    val otherOrganizationId = OrganizationId(UUID.randomUUID())
    givenSourceHasEnough(lowerRegion, otherOrganizationId)

    service.reallocateCapacity(otherOrganizationId, lowerRegion, higherRegion, 1.0)

    verify { allocatedCapacityRepository.subtractCapacityIfSufficient(otherOrganizationId.value, lowerRegion.value, 1.0) }
    verify { allocatedCapacityRepository.addCapacity(otherOrganizationId.value, higherRegion.value, 1.0) }
    verify(exactly = 0) { allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, any(), any()) }
    verify(exactly = 0) { allocatedCapacityRepository.addCapacity(organizationId.value, any(), any()) }
  }

  @Test
  fun `addCapacity adds to the default region`() {
    service.addCapacity(organizationId, 5.0)

    verify { allocatedCapacityRepository.addCapacity(organizationId.value, defaultRegion.value, 5.0) }
  }

  @Test
  fun `addCapacity does not touch any other region`() {
    service.addCapacity(organizationId, 5.0)

    verify(exactly = 0) { allocatedCapacityRepository.addCapacity(any(), lowerRegion.value, any()) }
    verify(exactly = 0) { allocatedCapacityRepository.subtractCapacityIfSufficient(any(), any(), any()) }
  }

  @Test
  fun `addCapacity rejects a zero amount`() {
    assertThrows(BadRequestProblem::class.java) { service.addCapacity(organizationId, 0.0) }

    verifyNothingWasWritten()
  }

  @Test
  fun `addCapacity rejects a negative amount`() {
    assertThrows(BadRequestProblem::class.java) { service.addCapacity(organizationId, -1.0) }

    verifyNothingWasWritten()
  }

  @Test
  fun `addCapacity rejects a NaN amount`() {
    assertThrows(BadRequestProblem::class.java) { service.addCapacity(organizationId, Double.NaN) }

    verifyNothingWasWritten()
  }

  @Test
  fun `addCapacity rejects an infinite amount`() {
    assertThrows(BadRequestProblem::class.java) { service.addCapacity(organizationId, Double.POSITIVE_INFINITY) }

    verifyNothingWasWritten()
  }

  @Test
  fun `addCapacity rejects an amount larger than the capacity column can hold`() {
    assertThrows(BadRequestProblem::class.java) { service.addCapacity(organizationId, Float.MAX_VALUE.toDouble() * 2) }

    verifyNothingWasWritten()
  }

  @Test
  fun `addCapacity scopes the write to the organization`() {
    val otherOrganizationId = OrganizationId(UUID.randomUUID())

    service.addCapacity(otherOrganizationId, 1.0)

    verify { allocatedCapacityRepository.addCapacity(otherOrganizationId.value, defaultRegion.value, 1.0) }
    verify(exactly = 0) { allocatedCapacityRepository.addCapacity(organizationId.value, any(), any()) }
  }

  @Test
  fun `removeCapacity subtracts from the given region`() {
    givenSourceHasEnough(lowerRegion)

    service.removeCapacity(organizationId, lowerRegion, 4.0)

    verify { allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, lowerRegion.value, 4.0) }
    verify(exactly = 0) { allocatedCapacityRepository.addCapacity(any(), any(), any()) }
  }

  @Test
  fun `removeCapacity rejects a zero amount`() {
    assertThrows(BadRequestProblem::class.java) { service.removeCapacity(organizationId, lowerRegion, 0.0) }

    verifyNothingWasWritten()
  }

  @Test
  fun `removeCapacity rejects a negative amount`() {
    assertThrows(BadRequestProblem::class.java) { service.removeCapacity(organizationId, lowerRegion, -1.0) }

    verifyNothingWasWritten()
  }

  @Test
  fun `removeCapacity rejects a NaN amount`() {
    assertThrows(BadRequestProblem::class.java) { service.removeCapacity(organizationId, lowerRegion, Double.NaN) }

    verifyNothingWasWritten()
  }

  @Test
  fun `removeCapacity rejects an infinite amount`() {
    assertThrows(BadRequestProblem::class.java) {
      service.removeCapacity(organizationId, lowerRegion, Double.POSITIVE_INFINITY)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `removeCapacity rejects an amount larger than the capacity column can hold`() {
    assertThrows(BadRequestProblem::class.java) {
      service.removeCapacity(organizationId, lowerRegion, Float.MAX_VALUE.toDouble() * 2)
    }

    verifyNothingWasWritten()
  }

  @Test
  fun `removeCapacity rejects a region that does not exist`() {
    every { dataplaneGroupRepository.findById(lowerRegion.value) } returns Optional.empty()

    assertThrows(BadRequestProblem::class.java) { service.removeCapacity(organizationId, lowerRegion, 1.0) }

    verifyNothingWasWritten()
  }

  @Test
  fun `removeCapacity allows taking capacity out of a deleted region`() {
    givenRegionExists(lowerRegion, tombstone = true)
    givenSourceHasEnough(lowerRegion)

    service.removeCapacity(organizationId, lowerRegion, 3.0)

    verify { allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, lowerRegion.value, 3.0) }
  }

  @Test
  fun `removeCapacity fails as a bad request when the region holds less than the amount`() {
    // 0 rows changed covers both "not enough capacity" and "no capacity there at all".
    every {
      allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, lowerRegion.value, 3.0)
    } returns 0

    assertThrows(BadRequestProblem::class.java) { service.removeCapacity(organizationId, lowerRegion, 3.0) }
  }

  @Test
  fun `removeCapacity fails as a bad request when the organization holds nothing in the region`() {
    every {
      allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, higherRegion.value, 1.0)
    } returns 0

    assertThrows(BadRequestProblem::class.java) { service.removeCapacity(organizationId, higherRegion, 1.0) }
  }

  @Test
  fun `removeCapacity scopes the write to the organization`() {
    val otherOrganizationId = OrganizationId(UUID.randomUUID())
    givenSourceHasEnough(lowerRegion, otherOrganizationId)

    service.removeCapacity(otherOrganizationId, lowerRegion, 1.0)

    verify { allocatedCapacityRepository.subtractCapacityIfSufficient(otherOrganizationId.value, lowerRegion.value, 1.0) }
    verify(exactly = 0) { allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, any(), any()) }
  }

  private fun givenDefaultRegionIs(dataplaneGroupId: DataplaneGroupId) {
    every { dataplaneGroupService.getDefaultDataplaneGroup() } returns
      ConfigDataplaneGroup().apply { id = dataplaneGroupId.value }
  }

  private fun givenRegionExists(
    dataplaneGroupId: DataplaneGroupId,
    tombstone: Boolean = false,
  ) {
    every { dataplaneGroupRepository.findById(dataplaneGroupId.value) } returns
      Optional.of(
        DataplaneGroup(
          id = dataplaneGroupId.value,
          organizationId = organizationId.value,
          name = "region-${dataplaneGroupId.value}",
          enabled = true,
          tombstone = tombstone,
        ),
      )
  }

  private fun givenSourceHasEnough(
    dataplaneGroupId: DataplaneGroupId,
    organizationId: OrganizationId = this.organizationId,
  ) {
    every {
      allocatedCapacityRepository.subtractCapacityIfSufficient(organizationId.value, dataplaneGroupId.value, any())
    } returns 1
  }

  private fun verifyNothingWasWritten() {
    verify(exactly = 0) { allocatedCapacityRepository.subtractCapacityIfSufficient(any(), any(), any()) }
    verify(exactly = 0) { allocatedCapacityRepository.addCapacity(any(), any(), any()) }
  }

  private fun allocationRow(
    dataplaneGroupId: DataplaneGroupId,
    allocatedCapacity: Double,
  ) = DataWorkerAllocatedCapacity(
    organizationId = organizationId.value,
    dataplaneGroupId = dataplaneGroupId.value,
    allocatedCapacity = allocatedCapacity,
  )
}

/**
 * Runs the callback without a real transaction. Named distinctly from the equivalent fake in
 * [DataWorkerCapacityServiceTest] because private top-level classes still share a JVM name.
 */
private class ImmediateAllocationTransactionOperations : TransactionOperations<Connection> {
  override fun getConnection(): Connection = mockk(relaxed = true)

  override fun hasConnection(): Boolean = true

  override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

  override fun <R : Any?> execute(
    definition: TransactionDefinition,
    callback: TransactionCallback<Connection, R>,
  ): R = callback.call(mockk(relaxed = true))
}
