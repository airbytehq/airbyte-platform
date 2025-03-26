/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.commons.constants.DataplaneConstantsKt;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebBackendGeographiesHandlerTest {

  private WebBackendGeographiesHandler webBackendGeographiesHandler;

  @BeforeEach
  void setUp() {
    webBackendGeographiesHandler = new WebBackendGeographiesHandler();
  }

  @Test
  void testListGeographiesOSS() {
    final WebBackendGeographiesListResult expected = new WebBackendGeographiesListResult().geographies(
        List.of(DataplaneConstantsKt.GEOGRAPHY_AUTO));

    final WebBackendGeographiesListResult actual = webBackendGeographiesHandler.listGeographiesOSS();

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void testListGeographiesCloud() {
    final WebBackendGeographiesListResult expected = new WebBackendGeographiesListResult().geographies(
        List.of(DataplaneConstantsKt.GEOGRAPHY_US, DataplaneConstantsKt.GEOGRAPHY_EU));

    final WebBackendGeographiesListResult actual = webBackendGeographiesHandler.listGeographiesCloud();

    Assertions.assertEquals(expected, actual);
  }

}
