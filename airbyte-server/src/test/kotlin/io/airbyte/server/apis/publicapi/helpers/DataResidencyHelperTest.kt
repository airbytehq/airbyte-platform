/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.constants.US_DATAPLANE_GROUP
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class DataResidencyHelperTest {
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  fun setup() {
    dataplaneGroupService = mockk()
  }

  @Test
  fun `getDataplaneGroupFromResidencyAndAirbyteEdition returns null when dataResidency is null`() {
    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)
    val result = helper.getDataplaneGroupFromResidencyAndAirbyteEdition(null)
    Assertions.assertNull(result)
  }

  @Test
  fun `getDataplaneGroupFromResidencyAndAirbyteEdition cloud edition with AUTO returns US group`() {
    val cloudDataplaneGroup = DataplaneGroup().withId(UUID.randomUUID())
    every { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP) } returns cloudDataplaneGroup

    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)

    listOf("auto", "AUTO", "AuTo").forEach { input ->
      val result = helper.getDataplaneGroupFromResidencyAndAirbyteEdition(input)
      Assertions.assertEquals(cloudDataplaneGroup.id, result?.id, "Expected default for input: $input")
    }

    verify { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP) }
  }

  @Test
  fun `getDataplaneGroupFromResidencyAndAirbyteEdition cloud edition with US returns US group`() {
    val cloudDataplaneGroup = DataplaneGroup().withId(UUID.randomUUID())

    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)

    listOf("us", "US", "Us").forEach { input ->
      every { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, input) } returns cloudDataplaneGroup

      val result = helper.getDataplaneGroupFromResidencyAndAirbyteEdition(input)
      Assertions.assertEquals(cloudDataplaneGroup.id, result?.id, "Expected US for input: $input")
    }

    verify { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP) }
  }

  @Test
  fun `getDataplaneGroupFromResidencyAndAirbyteEdition cloud edition with custom value returns custom value`() {
    val cloudDataplaneGroup = DataplaneGroup().withId(UUID.randomUUID())
    val customValue = "eu-west"

    every { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, customValue) } returns cloudDataplaneGroup

    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)

    val result = helper.getDataplaneGroupFromResidencyAndAirbyteEdition(customValue)
    Assertions.assertEquals(cloudDataplaneGroup.id, result?.id)

    verify { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, customValue) }
  }

  @Test
  fun `getDataplaneGroupFromResidencyAndAirbyteEdition OSS edition returns default dataplane group`() {
    val ossDataplaneGroup = DataplaneGroup().withId(UUID.randomUUID())
    every { dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(AirbyteEdition.COMMUNITY) } returns ossDataplaneGroup

    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.COMMUNITY)
    for (input in listOf("us", "US", "Us", "auto", "AUTO", "Auto", "eu-central", "Asia")) {
      val result = helper.getDataplaneGroupFromResidencyAndAirbyteEdition(input)
      Assertions.assertEquals(ossDataplaneGroup.id, result?.id, "Expected value for input: $input")
    }

    verify { dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(AirbyteEdition.COMMUNITY) }
  }

  @Test
  fun `getDataplaneGroupFromResidencyAndAirbyteEdition Enterprise edition returns custom dataplane group`() {
    val cloudDataplaneGroup = DataplaneGroup().withId(UUID.randomUUID())
    val customValue = "eu-west"

    every { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, customValue) } returns cloudDataplaneGroup

    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.ENTERPRISE)

    val result = helper.getDataplaneGroupFromResidencyAndAirbyteEdition(customValue)
    Assertions.assertEquals(cloudDataplaneGroup.id, result?.id)

    verify { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, customValue) }
  }
}
