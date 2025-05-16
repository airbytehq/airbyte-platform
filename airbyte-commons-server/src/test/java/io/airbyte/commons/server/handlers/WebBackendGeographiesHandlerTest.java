/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.commons.constants.OrganizationConstantsKt;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.data.services.DataplaneGroupService;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebBackendGeographiesHandlerTest {

  private WebBackendGeographiesHandler webBackendGeographiesHandler;
  private DataplaneGroupService dataplaneGroupService;

  @BeforeEach
  void setUp() {
    dataplaneGroupService = mock(DataplaneGroupService.class);
    webBackendGeographiesHandler = new WebBackendGeographiesHandler(dataplaneGroupService);
  }

  @Test
  void testListGeographies() {
    final UUID mockOrganizationId = UUID.randomUUID();
    final List<DataplaneGroup> dataplaneGroups = List.of(new DataplaneGroup().withName("test"));

    final WebBackendGeographiesListResult expected = new WebBackendGeographiesListResult().geographies(
        List.of("test"));

    when(dataplaneGroupService.listDataplaneGroups(
        List.of(OrganizationConstantsKt.getDEFAULT_ORGANIZATION_ID(), mockOrganizationId), false))
            .thenReturn(dataplaneGroups);

    final WebBackendGeographiesListResult actual = webBackendGeographiesHandler.listGeographies(mockOrganizationId);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void testGetDataplaneGroupNames_combinesAndDeduplicates() {
    final UUID orgId = UUID.randomUUID();

    final List<DataplaneGroup> dataplaneGroups = List.of(
        new DataplaneGroup().withName("US"),
        new DataplaneGroup().withName("EU"),
        new DataplaneGroup().withName("AUS"));

    when(dataplaneGroupService.listDataplaneGroups(
        Arrays.asList(OrganizationConstantsKt.getDEFAULT_ORGANIZATION_ID(), orgId), false))
            .thenReturn(dataplaneGroups);

    List<String> result = webBackendGeographiesHandler.getDataplaneGroupNames(orgId);

    Assertions.assertEquals(List.of("US", "EU", "AUS"), result);
  }

}
