/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.constants.GEOGRAPHY_US
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DataResidencyHelperTest {
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  fun setup() {
    dataplaneGroupService = mockk()
  }

  @Test
  fun `returns null when dataResidency is null`() {
    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)
    val result = helper.getDataplaneGroupNameFromResidencyAndAirbyteEdition(null)
    Assertions.assertNull(result)
  }

  @Test
  fun `cloud edition with AUTO returns default group`() {
    val defaultGroup = DataplaneGroup().withName("cloud-default")
    every { dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, GEOGRAPHY_US) } returns defaultGroup

    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)

    listOf("auto", "AUTO", "AuTo").forEach { input ->
      val result = helper.getDataplaneGroupNameFromResidencyAndAirbyteEdition(input)
      Assertions.assertEquals("cloud-default", result, "Expected default for input: $input")
    }
  }

  @Test
  fun `cloud edition with US returns US (case insensitive)`() {
    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)

    listOf("us", "US", "Us").forEach { input ->
      val result = helper.getDataplaneGroupNameFromResidencyAndAirbyteEdition(input)
      Assertions.assertEquals(input, result, "Expected US for input: $input")
    }
  }

  @Test
  fun `cloud edition with custom value returns custom value`() {
    val helper = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.CLOUD)
    val customValue = "eu-west"
    val result = helper.getDataplaneGroupNameFromResidencyAndAirbyteEdition(customValue)
    Assertions.assertEquals(customValue, result)
  }

  @Test
  fun `OSS editions return original value and log warning (case insensitive)`() {
    val editions = listOf(AirbyteEdition.COMMUNITY, AirbyteEdition.ENTERPRISE)
    val inputs = listOf("us", "US", "Us", "auto", "AUTO", "Auto", "eu-central", "Asia")

    for (edition in editions) {
      val helper = DataResidencyHelper(dataplaneGroupService, edition)
      for (input in inputs) {
        val result = helper.getDataplaneGroupNameFromResidencyAndAirbyteEdition(input)
        Assertions.assertEquals(input, result, "$edition expected original value for input: $input")
      }
    }
  }

  @Test
  fun `OSS editions with custom value return custom value`() {
    val customValue = "asia-east"

    val helperCommunity = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.COMMUNITY)
    val resultCommunity = helperCommunity.getDataplaneGroupNameFromResidencyAndAirbyteEdition(customValue)
    Assertions.assertEquals(customValue, resultCommunity)

    val helperEnterprise = DataResidencyHelper(dataplaneGroupService, AirbyteEdition.ENTERPRISE)
    val resultEnterprise = helperEnterprise.getDataplaneGroupNameFromResidencyAndAirbyteEdition(customValue)
    Assertions.assertEquals(customValue, resultEnterprise)
  }
}
