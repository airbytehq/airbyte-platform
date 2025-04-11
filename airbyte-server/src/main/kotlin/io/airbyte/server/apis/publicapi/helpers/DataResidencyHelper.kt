/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.constants.GEOGRAPHY_AUTO
import io.airbyte.commons.constants.GEOGRAPHY_US
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.data.services.DataplaneGroupService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class DataResidencyHelper(
  private val dataplaneGroupService: DataplaneGroupService,
  private val airbyteEdition: AirbyteEdition,
) {
  fun getDataplaneGroupNameFromResidencyAndAirbyteEdition(dataResidency: String?): String? {
    if (dataResidency == null) {
      return null
    } else {
      return if (airbyteEdition == AirbyteEdition.CLOUD) {
        if (dataResidency.equals(GEOGRAPHY_AUTO, ignoreCase = true)) {
          val default = dataplaneGroupService.getDataplaneGroupByOrganizationIdAndGeography(DEFAULT_ORGANIZATION_ID, GEOGRAPHY_US).name
          logger.warn { "Data Residency $dataResidency is no longer supported on Airbyte Cloud. Defaulting to $default." }
          default
        } else {
          dataResidency
        }
      } else {
        logger.warn { "Ignoring value for dataResidency=$dataResidency. dataResidency is not supported on $airbyteEdition." }
        dataResidency
      }
    }
  }
}
