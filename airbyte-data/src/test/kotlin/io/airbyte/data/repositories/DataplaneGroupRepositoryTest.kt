/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.config.Geography
import io.airbyte.data.repositories.entities.DataplaneGroup
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
class DataplaneGroupRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making an organization and user
      jooqDslContext
        .alterTable(
          Tables.DATAPLANE_GROUP,
        ).dropForeignKey(Keys.DATAPLANE_GROUP__DATAPLANE_GROUP_ORGANIZATION_ID_FKEY.constraint())
        .execute()
      jooqDslContext.alterTable(Tables.DATAPLANE_GROUP).dropForeignKey(Keys.DATAPLANE_GROUP__DATAPLANE_GROUP_UPDATED_BY_FKEY.constraint()).execute()
    }
  }

  @Test
  fun `create and retrieve dataplane group`() {
    val dataplaneGroup =
      DataplaneGroup(
        organizationId = UUID.randomUUID(),
        name = Geography.AUTO.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
      )
    val savedDataplaneGroup = dataplaneGroupRepository.save(dataplaneGroup)

    val retrievedDataplaneGroup = dataplaneGroupRepository.findById(savedDataplaneGroup.id)
    assertTrue(retrievedDataplaneGroup.isPresent)
    assertThat(retrievedDataplaneGroup.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(dataplaneGroup)
  }

  @Test
  fun `update dataplane group`() {
    val updatedName = Geography.US.name
    val updatedEnabled = true
    val dataplaneGroup =
      DataplaneGroup(
        organizationId = UUID.randomUUID(),
        name = Geography.AUTO.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
      )
    val savedDataplaneGroup = dataplaneGroupRepository.save(dataplaneGroup)

    dataplaneGroup.name = updatedName
    dataplaneGroup.enabled = updatedEnabled

    dataplaneGroupRepository.update(dataplaneGroup)

    val retrievedDataplaneGroup = dataplaneGroupRepository.findById(savedDataplaneGroup.id)
    assertTrue(retrievedDataplaneGroup.isPresent)
    assertEquals(updatedName, retrievedDataplaneGroup.get().name)
    assertEquals(updatedEnabled, retrievedDataplaneGroup.get().enabled)
  }

  @Test
  fun `delete dataplane group`() {
    val dataplaneGroup =
      DataplaneGroup(
        organizationId = UUID.randomUUID(),
        name = Geography.AUTO.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
      )
    val savedDataplaneGroup = dataplaneGroupRepository.save(dataplaneGroup)

    dataplaneGroupRepository.delete(dataplaneGroup)

    assertTrue(dataplaneGroupRepository.findById(savedDataplaneGroup.id).isEmpty)
  }

  @Test
  fun `list dataplane groups by organization id including tombstoned`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val dataplaneGroup1 =
      DataplaneGroup(
        organizationId = organizationId,
        name = Geography.AUTO.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplaneGroup2 =
      DataplaneGroup(
        organizationId = organizationId,
        name = Geography.US.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplaneGroup3 =
      DataplaneGroup(
        organizationId = otherOrganizationId,
        name = Geography.EU.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneGroupRepository.save(dataplaneGroup1)
    dataplaneGroupRepository.save(dataplaneGroup2)
    dataplaneGroupRepository.save(dataplaneGroup3)

    val retrievedDataplaneGroups = dataplaneGroupRepository.findAllByOrganizationIdOrderByUpdatedAtDesc(organizationId)

    assertEquals(2, retrievedDataplaneGroups.size)
    assertThat(retrievedDataplaneGroups)
      .extracting("name")
      .containsExactly(Geography.US.name, Geography.AUTO.name)
  }

  @Test
  fun `list dataplane groups by organization id without tombstoned`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val dataplaneGroup1 =
      DataplaneGroup(
        organizationId = organizationId,
        name = Geography.AUTO.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplaneGroup2 =
      DataplaneGroup(
        organizationId = organizationId,
        name = Geography.EU.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplaneGroup3 =
      DataplaneGroup(
        organizationId = otherOrganizationId,
        name = Geography.US.name,
        enabled = false,
        updatedBy = UUID.randomUUID(),
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    dataplaneGroupRepository.save(dataplaneGroup1)
    dataplaneGroupRepository.save(dataplaneGroup2)
    dataplaneGroupRepository.save(dataplaneGroup3)

    val retrievedDataplaneGroups = dataplaneGroupRepository.findAllByOrganizationIdAndTombstoneFalseOrderByUpdatedAtDesc(organizationId)

    assertEquals(1, retrievedDataplaneGroups.size)
    assertThat(retrievedDataplaneGroups)
      .extracting("name")
      .containsExactly(Geography.AUTO.name)
  }
}
