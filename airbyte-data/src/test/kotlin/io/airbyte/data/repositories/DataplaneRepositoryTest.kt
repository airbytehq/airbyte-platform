/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Dataplane
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest
class DataplaneRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // drop foreign keys to allow tests that create entities without all required relationships
      jooqDslContext
        .alterTable(
          Tables.DATAPLANE,
        ).dropForeignKey(Keys.DATAPLANE__DATAPLANE_DATAPLANE_GROUP_ID_FKEY.constraint())
        .execute()

      jooqDslContext
        .alterTable(Tables.DATAPLANE_GROUP)
        .dropForeignKey(Keys.DATAPLANE_GROUP__DATAPLANE_GROUP_ORGANIZATION_ID_FKEY.constraint())
        .execute()

      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_USER_ID_FKEY.constraint()).execute()
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_ORGANIZATION_ID_FKEY.constraint()).execute()
    }
  }

  @Test
  fun `create and retrieve dataplane`() {
    val dataplane =
      Dataplane(
        dataplaneGroupId = UUID.randomUUID(),
        name = "Test",
        enabled = false,
        tombstone = false,
      )
    val savedDataplane = dataplaneRepository.save(dataplane)

    val retrievedDataplane = dataplaneRepository.findById(savedDataplane.id)
    assertTrue(retrievedDataplane.isPresent)
    assertThat(retrievedDataplane.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(dataplane)
  }

  @Test
  fun `update dataplane`() {
    val updatedName = "Updated dataplane name"
    val updatedEnabled = true
    val dataplane =
      Dataplane(
        dataplaneGroupId = UUID.randomUUID(),
        name = "Test",
        enabled = false,
        tombstone = false,
      )
    val savedDataplane = dataplaneRepository.save(dataplane)

    dataplane.name = updatedName
    dataplane.enabled = updatedEnabled

    dataplaneRepository.update(dataplane)

    val retrievedDataplane = dataplaneRepository.findById(savedDataplane.id)
    assertTrue(retrievedDataplane.isPresent)
    assertEquals(updatedName, retrievedDataplane.get().name)
    assertEquals(updatedEnabled, retrievedDataplane.get().enabled)
  }

  @Test
  fun `delete dataplane`() {
    val dataplane =
      Dataplane(
        dataplaneGroupId = UUID.randomUUID(),
        name = "Test",
        enabled = false,
        tombstone = false,
      )
    val savedDataplane = dataplaneRepository.save(dataplane)

    dataplaneRepository.delete(dataplane)

    assertTrue(dataplaneRepository.findById(savedDataplane.id).isEmpty)
  }

  @Test
  fun `list dataplanes by dataplane group id including tombstoned`() {
    val dataplaneGroupId = UUID.randomUUID()
    val otherDataplaneGroupId = UUID.randomUUID()
    val dataplane1 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId,
        name = "Test 1",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplane2 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId,
        name = "Test 2",
        enabled = false,
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplane3 =
      Dataplane(
        dataplaneGroupId = otherDataplaneGroupId,
        name = "Test 3",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneRepository.save(dataplane1)
    dataplaneRepository.save(dataplane2)
    dataplaneRepository.save(dataplane3)

    val retrievedDataplanes = dataplaneRepository.findAllByDataplaneGroupIdOrderByUpdatedAtDesc(dataplaneGroupId)

    assertEquals(2, retrievedDataplanes.size)
    assertThat(retrievedDataplanes)
      .extracting("name")
      .containsExactly("Test 2", "Test 1")
  }

  @Test
  fun `list dataplanes by dataplane group id without tombstoned`() {
    val dataplaneGroupId = UUID.randomUUID()
    val otherDataplaneGroupId = UUID.randomUUID()
    val dataplane1 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId,
        name = "Test 1",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplane2 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId,
        name = "Test 2",
        enabled = false,
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplane3 =
      Dataplane(
        dataplaneGroupId = otherDataplaneGroupId,
        name = "Test 3",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneRepository.save(dataplane1)
    dataplaneRepository.save(dataplane2)
    dataplaneRepository.save(dataplane3)

    val retrievedDataplanes = dataplaneRepository.findAllByDataplaneGroupIdAndTombstoneFalseOrderByUpdatedAtDesc(dataplaneGroupId)

    assertEquals(1, retrievedDataplanes.size)
    assertThat(retrievedDataplanes)
      .extracting("name")
      .containsExactly("Test 1")
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `list dataplanes by multiple dataplane group ids`(withTombstoned: Boolean) {
    val dataplaneGroupId1 = UUID.randomUUID()
    val dataplaneGroupId2 = UUID.randomUUID()
    val otherDataplaneGroupId = UUID.randomUUID()
    val dataplane1 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId1,
        name = "Test 1",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplane2 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId1,
        name = "Test 2",
        enabled = false,
        tombstone = true, // should not be returned because it's tombstoned
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplane3 =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId2,
        name = "Test 3",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now().plusSeconds(2),
      )
    val dataplane4 =
      Dataplane(
        dataplaneGroupId = otherDataplaneGroupId, // should not be returned because it's in a different group
        name = "Test 4",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneRepository.save(dataplane1)
    dataplaneRepository.save(dataplane2)
    dataplaneRepository.save(dataplane3)
    dataplaneRepository.save(dataplane4)

    val retrievedDataplanes =
      dataplaneRepository.findAllByDataplaneGroupIds(
        listOf(dataplaneGroupId1, dataplaneGroupId2),
        withTombstoned,
      )

    if (withTombstoned) {
      assertEquals(3, retrievedDataplanes.size)
      assertThat(retrievedDataplanes)
        .extracting("name")
        .containsExactly("Test 3", "Test 2", "Test 1")
    } else {
      assertEquals(2, retrievedDataplanes.size)
      assertThat(retrievedDataplanes)
        .extracting("name")
        .containsExactly("Test 3", "Test 1")
    }
  }

  @Nested
  inner class FindAllByOrganizationIdsTests {
    @BeforeEach
    fun cleanupData() {
      dataplaneRepository.deleteAll()
      dataplaneGroupRepository.deleteAll()
    }

    private fun createDataplaneGroup(
      organizationId: UUID,
      name: String,
    ): DataplaneGroup =
      DataplaneGroup(
        organizationId = organizationId,
        name = name,
        enabled = true,
        tombstone = false,
      )

    private fun createDataplane(
      dataplaneGroupId: UUID,
      name: String,
      tombstone: Boolean = false,
      offsetSeconds: Long = 0,
    ): Dataplane =
      Dataplane(
        dataplaneGroupId = dataplaneGroupId,
        name = name,
        enabled = true,
        tombstone = tombstone,
        updatedAt = OffsetDateTime.now().plusSeconds(offsetSeconds),
      )

    @Test
    fun `returns dataplanes from specified organizations`() {
      val org1 = UUID.randomUUID()
      val org2 = UUID.randomUUID()
      val org3 = UUID.randomUUID()

      val dpg1 = createDataplaneGroup(org1, "Group 1")
      val dpg2 = createDataplaneGroup(org2, "Group 2")
      val dpg3 = createDataplaneGroup(org3, "Group 3")
      val dpg1Id = dataplaneGroupRepository.save(dpg1).id!!
      val dpg2Id = dataplaneGroupRepository.save(dpg2).id!!
      val dpg3Id = dataplaneGroupRepository.save(dpg3).id!!

      val dp1 = createDataplane(dpg1Id, "Dataplane 1")
      val dp2 = createDataplane(dpg2Id, "Dataplane 2", offsetSeconds = 1)
      val dp3 = createDataplane(dpg3Id, "Dataplane 3", offsetSeconds = 2)
      dataplaneRepository.saveAll(listOf(dp1, dp2, dp3))

      val result = dataplaneRepository.findAllByOrganizationIds(listOf(org1, org2), withTombstone = false)

      assertEquals(2, result.size)
      assertThat(result)
        .extracting("name")
        .containsExactly("Dataplane 2", "Dataplane 1")
    }

    @Test
    fun `returns empty list when no organizations match`() {
      val org1 = UUID.randomUUID()
      val org2 = UUID.randomUUID()

      val dpg1 = createDataplaneGroup(org1, "Group 1")
      val dpg1Id = dataplaneGroupRepository.save(dpg1).id!!
      val dp1 = createDataplane(dpg1Id, "Dataplane 1")
      dataplaneRepository.save(dp1)

      val result = dataplaneRepository.findAllByOrganizationIds(listOf(org2), withTombstone = false)

      assertEquals(0, result.size)
    }

    @Test
    fun `handles tombstone filtering correctly`() {
      val org1 = UUID.randomUUID()

      val dpg1 = dataplaneGroupRepository.save(createDataplaneGroup(org1, "Group 1")).id!!

      val activeDataplane = createDataplane(dpg1, "Active Dataplane")
      val tombstonedDataplane = createDataplane(dpg1, "Tombstoned Dataplane", tombstone = true, offsetSeconds = 1)
      dataplaneRepository.saveAll(listOf(activeDataplane, tombstonedDataplane))

      val resultWithoutTombstones = dataplaneRepository.findAllByOrganizationIds(listOf(org1), withTombstone = false)
      assertEquals(1, resultWithoutTombstones.size)
      assertEquals("Active Dataplane", resultWithoutTombstones[0].name)

      val resultWithTombstones = dataplaneRepository.findAllByOrganizationIds(listOf(org1), withTombstone = true)
      assertEquals(2, resultWithTombstones.size)
      assertThat(resultWithTombstones)
        .extracting("name")
        .containsExactly("Tombstoned Dataplane", "Active Dataplane")
    }

    @Test
    fun `handles multiple organizations with different dataplane counts`() {
      val org1 = UUID.randomUUID()
      val org2 = UUID.randomUUID()

      val dpg1 = dataplaneGroupRepository.save(createDataplaneGroup(org1, "Group 1")).id!!
      val dpg2 = dataplaneGroupRepository.save(createDataplaneGroup(org2, "Group 2")).id!!

      val dp1 = createDataplane(dpg1, "Dataplane 1", offsetSeconds = 0)
      val dp2 = createDataplane(dpg1, "Dataplane 2", offsetSeconds = 1)
      val dp3 = createDataplane(dpg2, "Dataplane 3", offsetSeconds = 2)
      dataplaneRepository.saveAll(listOf(dp1, dp2, dp3))

      val result = dataplaneRepository.findAllByOrganizationIds(listOf(org1, org2), withTombstone = false)

      assertEquals(3, result.size)
      assertThat(result)
        .extracting("name")
        .containsExactly("Dataplane 3", "Dataplane 2", "Dataplane 1")
    }
  }
}
