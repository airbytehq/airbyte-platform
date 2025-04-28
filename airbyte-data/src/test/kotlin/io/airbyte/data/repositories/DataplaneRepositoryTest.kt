/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Dataplane
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest
class DataplaneRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making a dataplane group
      jooqDslContext
        .alterTable(
          Tables.DATAPLANE,
        ).dropForeignKey(Keys.DATAPLANE__DATAPLANE_DATAPLANE_GROUP_ID_FKEY.constraint())
        .execute()
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
  fun `list dataplanes by organization id including tombstoned`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val dataplane1 =
      Dataplane(
        dataplaneGroupId = organizationId,
        name = "Test 1",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplane2 =
      Dataplane(
        dataplaneGroupId = organizationId,
        name = "Test 2",
        enabled = false,
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplane3 =
      Dataplane(
        dataplaneGroupId = otherOrganizationId,
        name = "Test 3",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneRepository.save(dataplane1)
    dataplaneRepository.save(dataplane2)
    dataplaneRepository.save(dataplane3)

    val retrievedDataplanes = dataplaneRepository.findAllByDataplaneGroupIdOrderByUpdatedAtDesc(organizationId)

    assertEquals(2, retrievedDataplanes.size)
    assertThat(retrievedDataplanes)
      .extracting("name")
      .containsExactly("Test 2", "Test 1")
  }

  @Test
  fun `list dataplanes by organization id without tombstoned`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val dataplane1 =
      Dataplane(
        dataplaneGroupId = organizationId,
        name = "Test 1",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplane2 =
      Dataplane(
        dataplaneGroupId = organizationId,
        name = "Test 2",
        enabled = false,
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplane3 =
      Dataplane(
        dataplaneGroupId = otherOrganizationId,
        name = "Test 3",
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneRepository.save(dataplane1)
    dataplaneRepository.save(dataplane2)
    dataplaneRepository.save(dataplane3)

    val retrievedDataplanes = dataplaneRepository.findAllByDataplaneGroupIdAndTombstoneFalseOrderByUpdatedAtDesc(organizationId)

    assertEquals(1, retrievedDataplanes.size)
    assertThat(retrievedDataplanes)
      .extracting("name")
      .containsExactly("Test 1")
  }
}
