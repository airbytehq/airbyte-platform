/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.Organization;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * A service that manages organizations.
 */
public interface OrganizationService {

  Optional<Organization> getOrganization(UUID organizationId) throws IOException;

  Optional<Organization> getOrganizationForWorkspaceId(UUID workspaceId) throws IOException;

  void writeOrganization(Organization organization) throws IOException;

}
