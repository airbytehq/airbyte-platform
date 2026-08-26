/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerAllocatedCapacity
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.kotest.matchers.doubles.plusOrMinus
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.micronaut.context.env.Environment
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
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

    saved.id.shouldNotBeNull()
    saved.createdAt.shouldNotBeNull()
    saved.updatedAt.shouldNotBeNull()
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

    found.map { it.dataplaneGroupId }.toSet() shouldBe setOf(usRegionId, euRegionId)
    found.sumOf { it.allocatedCapacity }.shouldBe(13.0 plusOrMinus TOLERANCE)
  }

  @Test
  fun `findByOrganizationId returns empty when the organization has no rows`() {
    dataWorkerAllocatedCapacityRepository.findByOrganizationId(UUID.randomUUID()) shouldBe emptyList()
  }

  @Test
  fun `findByOrganizationIdAndDataplaneGroupId returns the matching row`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 7.5))

    val found = dataWorkerAllocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId, usRegionId)

    found.shouldNotBeNull()
    found.allocatedCapacity.shouldBe(7.5 plusOrMinus TOLERANCE)
  }

  @Test
  fun `findByOrganizationIdAndDataplaneGroupId returns null when the region has no row`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 7.5))

    dataWorkerAllocatedCapacityRepository.findByOrganizationIdAndDataplaneGroupId(organizationId, euRegionId) shouldBe null
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

    updated shouldBe 1
    capacityOf(usRegionId).shouldBe(7.0 plusOrMinus TOLERANCE)
  }

  @Test
  fun `subtractCapacityIfSufficient allows taking the whole balance`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))

    val updated = dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 10.0)

    updated shouldBe 1
    capacityOf(usRegionId).shouldBe(0.0 plusOrMinus TOLERANCE)
  }

  @Test
  fun `subtractCapacityIfSufficient changes nothing when the region has too little`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 2.0))

    val updated = dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 5.0)

    updated shouldBe 0
    capacityOf(usRegionId).shouldBe(2.0 plusOrMinus TOLERANCE)
  }

  @Test
  fun `subtractCapacityIfSufficient reports no change when the region has no row`() {
    dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, euRegionId, 1.0) shouldBe 0
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

    dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 4.0) shouldBe 1

    capacityOf(usRegionId).shouldBe(6.0 plusOrMinus TOLERANCE)
    capacityOf(usRegionId, otherOrganizationId).shouldBe(10.0 plusOrMinus TOLERANCE)
  }

  @Test
  fun `addCapacity creates the row when the region has none`() {
    val written = dataWorkerAllocatedCapacityRepository.addCapacity(organizationId, euRegionId, 3.0)

    written shouldBe 1
    capacityOf(euRegionId).shouldBe(3.0 plusOrMinus TOLERANCE)
  }

  @Test
  fun `addCapacity increments an existing row`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(euRegionId, 3.0))

    val written = dataWorkerAllocatedCapacityRepository.addCapacity(organizationId, euRegionId, 2.5)

    written shouldBe 1
    capacityOf(euRegionId).shouldBe(5.5 plusOrMinus TOLERANCE)
    dataWorkerAllocatedCapacityRepository.findByOrganizationId(organizationId).size shouldBe 1
  }

  @Test
  fun `addCapacity moves updated_at forward`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(euRegionId, 3.0))
    val backdated = backdateUpdatedAt(euRegionId)

    dataWorkerAllocatedCapacityRepository.addCapacity(organizationId, euRegionId, 1.0)

    (updatedAtOf(euRegionId) > backdated) shouldBe true
  }

  @Test
  fun `subtractCapacityIfSufficient moves updated_at forward`() {
    dataWorkerAllocatedCapacityRepository.save(allocation(usRegionId, 10.0))
    val backdated = backdateUpdatedAt(usRegionId)

    dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(organizationId, usRegionId, 1.0)

    (updatedAtOf(usRegionId) > backdated) shouldBe true
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

  private fun updatedAtOf(dataplaneGroupId: UUID): OffsetDateTime =
    dataWorkerAllocatedCapacityRepository
      .findByOrganizationIdAndDataplaneGroupId(organizationId, dataplaneGroupId)
      .shouldNotBeNull()
      .updatedAt
      .shouldNotBeNull()

  private fun capacityOf(
    dataplaneGroupId: UUID,
    organizationId: UUID = this.organizationId,
  ): Double =
    dataWorkerAllocatedCapacityRepository
      .findByOrganizationIdAndDataplaneGroupId(organizationId, dataplaneGroupId)
      .shouldNotBeNull()
      .allocatedCapacity

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
