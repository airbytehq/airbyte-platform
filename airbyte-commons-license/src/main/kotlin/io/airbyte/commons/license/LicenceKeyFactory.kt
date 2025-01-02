/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.context.exceptions.DisabledBeanException
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * This factory provides the license key by preferring the `airbyte.license-key` property over the
 * deprecated `airbyte-yml.license-key` property. For backwards compatibility, if
 * `airbyte-yml.license-key` is not provided, this method falls back on `airbyte.license-key`.
 */
@Factory
class LicenceKeyFactory {
  @Singleton
  @Named("licenseKey")
  fun licenseKey(
    @Value("\${airbyte.license-key}") preferredLicenseKey: String?,
    @Value("\${airbyte-yml.license-key}") airbyteYmlLicenseKey: String?,
  ): String =
    preferredLicenseKey
      ?.takeIf { it.isNotEmpty() }
      ?: airbyteYmlLicenseKey
      ?: throw DisabledBeanException("License key not provided.")
}
