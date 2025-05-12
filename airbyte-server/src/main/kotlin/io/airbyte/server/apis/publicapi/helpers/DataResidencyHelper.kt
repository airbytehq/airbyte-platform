/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.commons.constants.AUTO_DATAPLANE_GROUP
import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.constants.US_DATAPLANE_GROUP
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class DataResidencyHelper(
  private val dataplaneGroupService: DataplaneGroupService,
  private val airbyteEdition: AirbyteEdition,
) {
  fun getDataplaneGroupFromResidencyAndAirbyteEdition(dataResidency: String?): DataplaneGroup? {
    if (dataResidency == null) {
      return null
    }
    return when (airbyteEdition) {
      AirbyteEdition.CLOUD -> {
        if (dataResidency.equals(AUTO_DATAPLANE_GROUP, ignoreCase = true)) {
          val default = dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP)
          logger.warn { "Data Residency $dataResidency is no longer supported on Airbyte Cloud. Defaulting to $default." }
          default
        } else {
          dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, dataResidency)
        }
      }

      AirbyteEdition.COMMUNITY -> {
        logger.warn { "Ignoring value for dataResidency=$dataResidency. dataResidency is not supported on $airbyteEdition." }
        dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(AirbyteEdition.COMMUNITY)
      }

      AirbyteEdition.ENTERPRISE -> {
        dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, dataResidency)
      }
    }
  }
}
