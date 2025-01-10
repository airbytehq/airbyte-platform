/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import io.airbyte.commons.license.AirbyteLicense.LicenseType
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import jakarta.inject.Singleton

/**
 * Bean that contains the Airbyte License that is retrieved from the licensing server at application
 * startup.
 */
@Singleton
@RequiresAirbyteProEnabled
class ActiveAirbyteLicense {
  var license: AirbyteLicense? = null

  val isPro: Boolean
    get() = license?.takeIf { it.type == LicenseType.PRO }?.let { true } ?: false
}
