/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.commons.constants.DataplaneConstantsKt;
import io.airbyte.commons.constants.OrganizationConstantsKt;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.data.services.DataplaneGroupService;
import java.util.Collections;
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
    final List<DataplaneGroup> dataplaneGroups = List.of(new DataplaneGroup().withName("AUTO"));

    final WebBackendGeographiesListResult expected = new WebBackendGeographiesListResult().geographies(
        List.of(DataplaneConstantsKt.GEOGRAPHY_AUTO));

    when(dataplaneGroupService.listDataplaneGroups(OrganizationConstantsKt.getDEFAULT_ORGANIZATION_ID(), false))
        .thenReturn(Collections.emptyList());
    when(dataplaneGroupService.listDataplaneGroups(mockOrganizationId, false))
        .thenReturn(dataplaneGroups);

    final WebBackendGeographiesListResult actual = webBackendGeographiesHandler.listGeographies(mockOrganizationId);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void testGetDataplaneGroupNames_combinesAndDeduplicates() {
    final UUID orgId = UUID.randomUUID();
    final UUID defaultOrgId = OrganizationConstantsKt.getDEFAULT_ORGANIZATION_ID();

    final List<DataplaneGroup> defaultGroups = List.of(
        new DataplaneGroup().withName("US"),
        new DataplaneGroup().withName("EU"));
    final List<DataplaneGroup> orgGroups = List.of(
        new DataplaneGroup().withName("EU"),
        new DataplaneGroup().withName("AUS"));

    when(dataplaneGroupService.listDataplaneGroups(defaultOrgId, false)).thenReturn(defaultGroups);
    when(dataplaneGroupService.listDataplaneGroups(orgId, false)).thenReturn(orgGroups);

    List<String> result = webBackendGeographiesHandler.getDataplaneGroupNames(orgId);

    Assertions.assertEquals(List.of("US", "EU", "AUS"), result);
  }

}
