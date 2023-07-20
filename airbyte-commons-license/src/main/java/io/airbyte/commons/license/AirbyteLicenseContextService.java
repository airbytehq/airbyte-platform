/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.micronaut.context.annotation.Context;
import lombok.extern.slf4j.Slf4j;

/**
 * Service that establishes the active Airbyte License. This is annotated with @Context so that it
 * occurs during application initialization, so that the license is available for downstream beans
 * to conditionally activate.
 */
@Context
@Slf4j
@RequiresAirbyteProEnabled
public class AirbyteLicenseContextService {

  public AirbyteLicenseContextService(final AirbyteLicenseFetcher airbyteLicenseFetcher, final ActiveAirbyteLicense activeAirbyteLicense) {
    activeAirbyteLicense.setLicense(airbyteLicenseFetcher.fetchLicense());
  }

}
