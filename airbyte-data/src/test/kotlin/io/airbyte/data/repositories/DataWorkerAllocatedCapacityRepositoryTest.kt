/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerAllocatedCapacity
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.context.env.Environment
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private const val TOLERANCE = 0.0001

@MicronautTest(environments = [Environment.TEST])
internal class DataWorkerAllocatedCapacityRepositoryTest : AbstractConfigRepositoryTest() {
  private val organizationId = UUID.randomUUID()
  private val usRegionId = UUID.randomUUID()
  private val euRegionId = UUID.randomUUID()

  @AfterEach
  fun cleanup() {
    dataWorkerAllocatedCapacityRepository.deleteAll()
  }

  @Test
  fun `save populates the id and both timestamps`() {
    val saved = dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))

    assertNotNull(saved.id)
    assertNotNull(saved.createdAt)
    assertNotNull(saved.updatedAt)
  }

  @Test
  fun `findByOrganizationId returns only that organization's rows`() {
    val otherOrganizationId = UUID.randomUUID()
    dataWorkerAllocatedCapacityRepository.saveAll(
      listOf(
        allocation(usRegionId, 10.0),
        allocation(euRegionId, 3.0),
        allocation(usRegionId, 99.0, organizationId = otherOrganizationId),
      ),
    )

    val found = dataWorkerAllocatedCapacityRepository.findByOrganizationId(organizationId)

    assertEquals(setOf(usRegionId, euRegionId), found.map { it.dataplaneGroupId }.toSet())
    assertEquals(13.0, found.sumOf { it.allocatedCapacity }, TOLERANCE)
  }

  @Test
  fun `findByOrganizationId returns empty when the organization has no rows`() {
    assertEquals(emptyList<Any>(), dataWorkerAllocatedCapacityRepository.findByOrganizationId(UUID.randomUUID()))
  }

  @Test
  fun `findByOrganizationIdAndDataplaneGroupId returns the matching row`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 7.5))

    val found = dataWorkerAllocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId, usRegionId)

    assertNotNull(found)
    assertEquals(7.5, found!!.allocatedCapacity, TOLERANCE)
  }

  @Test
  fun `findByOrganizationIdAndDataplaneGroupId returns null when the region has no row`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 7.5))

    assertNull(dataWorkerAllocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId, euRegionId))
  }

  @Test
  fun `a second row for the same organization and region is rejected`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))

    assertThrows<DataAccessException> {
      dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 4.0))
    }
  }

  @Test
  fun `negative capacity is rejected`() {
    assertThrows<DataAccessException> {
      dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, -1.0))
    }
  }

  @Test
  fun `subtractCapacityIfSufficient subtracts when the region has enough`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))

    val updated = dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 3.0)

    assertEquals(1, updated)
    assertEquals(7.0, capacityOf(usRegionId), TOLERANCE)
  }

  @Test
  fun `subtractCapacityIfSufficient allows taking the whole balance`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))

    val updated = dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 10.0)

    assertEquals(1, updated)
    assertEquals(0.0, capacityOf(usRegionId), TOLERANCE)
  }

  @Test
  fun `subtractCapacityIfSufficient changes nothing when the region has too little`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 2.0))

    val updated = dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 5.0)

    assertEquals(0, updated)
    assertEquals(2.0, capacityOf(usRegionId), TOLERANCE)
  }

  @Test
  fun `subtractCapacityIfSufficient reports no change when the region has no row`() {
    assertEquals(0, dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, euRegionId, 1.0))
  }

  @Test
  fun `subtractCapacityIfSufficient leaves another organization's row in the same region alone`() {
    val otherOrganizationId = UUID.randomUUID()
    dataWorkerAllocatedCapacityRepository.saveAll(
      listOf(
        allocation(usRegionId, 10.0),
        allocation(usRegionId, 10.0, organizationId = otherOrganizationId),
      ),
    )

    assertEquals(1, dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 4.0))

    assertEquals(6.0, capacityOf(usRegionId), TOLERANCE)
    assertEquals(10.0, capacityOf(usRegionId, otherOrganizationId), TOLERANCE)
  }

  @Test
  fun `addCapacity creates the row when the region has none`() {
    val written = dataWorkerAllocatedCapacityRepository.addCapacity(organizationId, euRegionId, 3.0)

    assertEquals(1, written)
    assertEquals(3.0, capacityOf(euRegionId), TOLERANCE)
  }

  @Test
  fun `addCapacity increments an existing row`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(euRegionId, 3.0))

    val written = dataWorkerAllocatedCapacityRepository.addCapacity(organizationId, euRegionId, 2.5)

    assertEquals(1, written)
    assertEquals(5.5, capacityOf(euRegionId), TOLERANCE)
    assertEquals(1, dataWorkerAllocatedCapacityRepository.findByOrganizationId(organizationId).size)
  }

  @Test
  fun `addCapacity moves updated_at forward`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(euRegionId, 3.0))
    val backdated = backdateUpdatedAt(euRegionId)

    dataWorkerAllocatedCapacityRepository.addCapacity(organizationId, euRegionId, 1.0)

    assertTrue(updatedAtOf(euRegionId) > backdated)
  }

  @Test
  fun `subtractCapacityIfSufficient moves updated_at forward`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))
    val backdated = backdateUpdatedAt(usRegionId)

    dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 1.0)

    assertTrue(updatedAtOf(usRegionId) > backdated)
  }

  /**
   * Rewinds `updated_at` a year in raw SQL and returns the value written.
   *
   * `@DateUpdated` timestamps come from the JVM clock while the custom queries use the database's
   * `NOW()`, so comparing a repository write against a query write compares two clocks. Rewinding
   * first keeps the assertion within one clock.
   */
  private fun backdateUpdatedAt(dataplaneGroupId: UUID): OffsetDateTime {
    val table = Tables.DATA_WORKER_ALLOCATED_CAPACITY
    jooqDslContext
      .update(table)
      .set(table.UPDATED_AT, OffsetDateTime.now(ZoneOffset.UTC).minusYears(1))
      .where(table.ORGANIZATION_ID.eq(organizationId))
      .and(table.DATAPLANE_GROUP_ID.eq(dataplaneGroupId))
      .execute()
    return updatedAtOf(dataplaneGroupId)
  }

  private fun updatedAtOf(dataplaneGroupId: UUID): OffsetDateTime {
    val row = dataWorkerAllocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId, dataplaneGroupId)
    assertNotNull(row)
    val updatedAt = row!!.updatedAt
    assertNotNull(updatedAt)
    return updatedAt!!
  }

  private fun capacityOf(
    dataplaneGroupId: UUID,
    organizationId: UUID = this.organizationId,
  ): Double {
    val row = dataWorkerAllocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId, dataplaneGroupId)
    assertNotNull(row)
    return row!!.allocatedCapacity
  }

  private fun allocation(
    dataplaneGroupId: UUID,
    allocatedCapacity: Double,
    organizationId: UUID = this.organizationId,
  ) = DataWorkerAllocatedCapacity(
    organizationId = organizationId,
    dataplaneGroupId = dataplaneGroupId,
    allocatedCapacity = allocatedCapacity,
  )
}
