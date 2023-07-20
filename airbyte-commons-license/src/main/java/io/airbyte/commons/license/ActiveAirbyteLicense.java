/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import io.airbyte.commons.license.AirbyteLicense.LicenseType;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Bean that contains the Airbyte License that is retrieved from the licensing server at application
 * startup.
 */
@Slf4j
@Singleton
@RequiresAirbyteProEnabled
public class ActiveAirbyteLicense {

  private Optional<AirbyteLicense> airbyteLicense;

  public void setLicense(final AirbyteLicense airbyteLicense) {
    this.airbyteLicense = Optional.ofNullable(airbyteLicense);
  }

  public Optional<AirbyteLicense> getLicense() {
    return airbyteLicense;
  }

  /**
   * Returns the type of the license. If no license is present, defaults to INVALID.
   */
  public LicenseType getLicenseType() {
    return airbyteLicense.map(AirbyteLicense::type).orElse(LicenseType.INVALID);
  }

  public boolean isPro() {
    return airbyteLicense.map(license -> license.type().equals(LicenseType.PRO)).orElse(false);
  }

}
