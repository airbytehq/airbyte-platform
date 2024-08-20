/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.exceptions.DisabledBeanException;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * This factory provides the license key by preferring the `airbyte.license-key` property over the
 * deprecated `airbyte-yml.license-key` property. For backwards compatibility, if
 * `airbyte-yml.license-key` is not provided, this method falls back on `airbyte.license-key`.
 */
@Factory
public class LicenceKeyFactory {

  @Singleton
  @Named("licenseKey")
  public String licenseKey(@Value("${airbyte.license-key}") final Optional<String> preferredLicenseKey,
                           @Value("${airbyte-yml.license-key}") final Optional<String> airbyteYmlLicenseKey) {
    return preferredLicenseKey.filter(StringUtils::isNotEmpty)
        .orElseGet(() -> airbyteYmlLicenseKey.orElseThrow(() -> new DisabledBeanException("License key not provided.")));
  }

}
