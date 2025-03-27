/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.constants.GEOGRAPHY_AUTO
import io.airbyte.commons.constants.GEOGRAPHY_EU
import io.airbyte.commons.constants.GEOGRAPHY_US
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
      // so we don't have to deal with making an organization
      jooqDslContext
        .alterTable(
          Tables.DATAPLANE_GROUP,
        ).dropForeignKey(Keys.DATAPLANE_GROUP__DATAPLANE_GROUP_ORGANIZATION_ID_FKEY.constraint())
        .execute()
    }
  }

  @Test
  fun `create and retrieve dataplane group`() {
    val dataplaneGroup =
      DataplaneGroup(
        organizationId = UUID.randomUUID(),
        name = GEOGRAPHY_AUTO,
        enabled = false,
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
    val updatedName = GEOGRAPHY_US
    val updatedEnabled = true
    val dataplaneGroup =
      DataplaneGroup(
        organizationId = UUID.randomUUID(),
        name = GEOGRAPHY_AUTO,
        enabled = false,
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
        name = GEOGRAPHY_AUTO,
        enabled = false,
        tombstone = false,
      )
    val savedDataplaneGroup = dataplaneGroupRepository.save(dataplaneGroup)

    dataplaneGroupRepository.delete(dataplaneGroup)

    assertTrue(dataplaneGroupRepository.findById(savedDataplaneGroup.id).isEmpty)
  }

  @Test
  fun `find dataplane groups by organization id and name`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val matchingName = GEOGRAPHY_AUTO
    val nonMatchingName = GEOGRAPHY_EU

    val dataplaneGroup1 =
      DataplaneGroup(
        organizationId = organizationId,
        name = matchingName,
        enabled = true,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )

    val dataplaneGroup2 =
      DataplaneGroup(
        organizationId = organizationId,
        name = nonMatchingName,
        enabled = true,
        tombstone = false,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )

    val dataplaneGroup3 =
      DataplaneGroup(
        organizationId = otherOrganizationId,
        name = matchingName,
        enabled = true,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )

    dataplaneGroupRepository.save(dataplaneGroup1)
    dataplaneGroupRepository.save(dataplaneGroup2)
    dataplaneGroupRepository.save(dataplaneGroup3)

    val retrievedDataplaneGroups = dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(organizationId, matchingName)

    assertEquals(1, retrievedDataplaneGroups.size)
    assertThat(retrievedDataplaneGroups)
      .extracting("name")
      .containsExactly(matchingName)
  }

  @Test
  fun `find dataplane groups by organization id and name ignores case`() {
    val organizationId = UUID.randomUUID()
    val matchingName1 = GEOGRAPHY_AUTO.uppercase()
    val matchingName2 = GEOGRAPHY_AUTO.lowercase()

    val dataplaneGroup =
      DataplaneGroup(
        organizationId = organizationId,
        name = GEOGRAPHY_AUTO,
        enabled = true,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )

    dataplaneGroupRepository.save(dataplaneGroup)

    val retrievedDataplaneGroups1 = dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(organizationId, matchingName1)

    assertEquals(1, retrievedDataplaneGroups1.size)
    assertThat(retrievedDataplaneGroups1)
      .extracting("name")
      .containsExactly(GEOGRAPHY_AUTO)

    val retrievedDataplaneGroups2 = dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(organizationId, matchingName1)

    assertEquals(1, retrievedDataplaneGroups2.size)
    assertThat(retrievedDataplaneGroups2)
      .extracting("name")
      .containsExactly(GEOGRAPHY_AUTO)
  }

  @Test
  fun `list dataplane groups by organization id including tombstoned`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val dataplaneGroup1 =
      DataplaneGroup(
        organizationId = organizationId,
        name = GEOGRAPHY_AUTO,
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplaneGroup2 =
      DataplaneGroup(
        organizationId = organizationId,
        name = GEOGRAPHY_US,
        enabled = false,
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplaneGroup3 =
      DataplaneGroup(
        organizationId = otherOrganizationId,
        name = GEOGRAPHY_EU,
        enabled = false,
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
      .containsExactly(GEOGRAPHY_US, GEOGRAPHY_AUTO)
  }

  @Test
  fun `list dataplane groups by organization id without tombstoned`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val dataplaneGroup1 =
      DataplaneGroup(
        organizationId = organizationId,
        name = GEOGRAPHY_AUTO,
        enabled = false,
        tombstone = false,
        updatedAt = OffsetDateTime.now(),
      )
    val dataplaneGroup2 =
      DataplaneGroup(
        organizationId = organizationId,
        name = GEOGRAPHY_EU,
        enabled = false,
        tombstone = true,
        updatedAt = OffsetDateTime.now().plusSeconds(1),
      )
    val dataplaneGroup3 =
      DataplaneGroup(
        organizationId = otherOrganizationId,
        name = GEOGRAPHY_US,
        enabled = false,
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
      .containsExactly(GEOGRAPHY_AUTO)
  }
}
