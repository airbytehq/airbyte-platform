/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

import io.airbyte.commons.server.services.shared.ResourcesByOrganizationQueryPaginated;
import io.airbyte.config.Organization;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A service that manages organizations.
 */
public interface OrganizationService {

  Optional<Organization> getOrganization(UUID organizationId) throws IOException;

  void writeOrganization(Organization organization) throws IOException;

  List<Organization> listOrganizations() throws IOException;

  Stream<Organization> listOrganizationQuery(Optional<UUID> organizationId) throws IOException;

  List<Organization> listOrganizationsPaginated(ResourcesByOrganizationQueryPaginated resourcesByOrganizationQueryPaginated) throws IOException;

  Optional<UUID> getOrganizationIdFromWorkspaceId(final UUID scopeId) throws IOException;

}
