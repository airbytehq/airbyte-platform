/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license

import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import jakarta.inject.Singleton

@Singleton
@RequiresAirbyteProEnabled
class AirbyteLicenseContextService(
  licenseReader: AirbyteLicenseReader,
  activeAirbyteLicense: ActiveAirbyteLicense,
) {
  init {
    activeAirbyteLicense.license = licenseReader.extractLicense()
  }
}
