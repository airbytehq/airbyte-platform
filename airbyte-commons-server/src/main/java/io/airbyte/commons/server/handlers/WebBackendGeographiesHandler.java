/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.commons.constants.DataplaneConstantsKt;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.Collections;

/**
 * The web backend is an abstraction that allows the frontend to structure data in such a way that
 * it is easier for a react frontend to consume. It should NOT have direct access to the database.
 * It should operate exclusively by calling other endpoints that are exposed in the API.
 *
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class WebBackendGeographiesHandler {

  public WebBackendGeographiesListResult listGeographiesOSS() {
    // for now, OSS only supports AUTO. This can evolve to account for complex OSS use cases, but for
    // now we expect OSS deployments to use a single default Task Queue for scheduling syncs in a vast
    // majority of cases.
    return new WebBackendGeographiesListResult().geographies(
        Collections.singletonList(DataplaneConstantsKt.GEOGRAPHY_AUTO));
  }

  /**
   * Only called by the wrapped Cloud API to enable multi-cloud.
   */
  public WebBackendGeographiesListResult listGeographiesCloud() {
    return new WebBackendGeographiesListResult().geographies(Arrays.asList(DataplaneConstantsKt.GEOGRAPHY_US, DataplaneConstantsKt.GEOGRAPHY_EU));
  }

}
