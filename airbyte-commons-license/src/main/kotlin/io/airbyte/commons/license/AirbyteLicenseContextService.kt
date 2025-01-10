/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.micronaut.context.annotation.Context

/**
 * Service that establishes the active Airbyte License. This is annotated with @Context so that it
 * occurs during application initialization, so that the license is available for downstream beans
 * to conditionally activate.
 */
@Context
@RequiresAirbyteProEnabled
class AirbyteLicenseContextService(
  licenseReader: AirbyteLicenseReader,
  activeAirbyteLicense: ActiveAirbyteLicense,
) {
  init {
    activeAirbyteLicense.license = licenseReader.extractLicense()
  }
}
