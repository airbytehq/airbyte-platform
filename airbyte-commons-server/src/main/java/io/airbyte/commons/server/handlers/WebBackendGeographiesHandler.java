/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.commons.constants.OrganizationConstantsKt;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.data.services.DataplaneGroupService;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The web backend is an abstraction that allows the frontend to structure data in such a way that
 * it is easier for a react frontend to consume. It should NOT have direct access to the database.
 * It should operate exclusively by calling other endpoints that are exposed in the API.
 *
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class WebBackendGeographiesHandler {

  private final DataplaneGroupService dataplaneGroupService;

  public WebBackendGeographiesHandler(final DataplaneGroupService dataplaneGroupService) {
    this.dataplaneGroupService = dataplaneGroupService;
  }

  public WebBackendGeographiesListResult listGeographies(final UUID organizationId) {
    return new WebBackendGeographiesListResult().geographies(getDataplaneGroupNames(organizationId));
  }

  List<String> getDataplaneGroupNames(final UUID organizationId) {
    final List<DataplaneGroup> defaultOrgGroups =
        dataplaneGroupService.listDataplaneGroups(OrganizationConstantsKt.getDEFAULT_ORGANIZATION_ID(), false);
    final List<DataplaneGroup> orgGroups =
        dataplaneGroupService.listDataplaneGroups(organizationId, false);

    return Stream.concat(defaultOrgGroups.stream(), orgGroups.stream())
        .map(DataplaneGroup::getName)
        .distinct()
        .collect(Collectors.toList());
  }

}
