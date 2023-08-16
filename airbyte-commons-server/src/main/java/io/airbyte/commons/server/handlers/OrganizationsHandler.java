/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Organization;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OrganizationHandler for handling organization resource related operation.
 *
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings("MissingJavadocMethod")
@Singleton
public class OrganizationsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OrganizationsHandler.class);
  private final PermissionPersistence permissionPersistence;
  private final OrganizationPersistence organizationPersistence;

  private final Supplier<UUID> uuidGenerator;

  @Inject
  public OrganizationsHandler(final OrganizationPersistence organizationPersistence,
                              final PermissionPersistence permissionPersistence,
                              final Supplier<UUID> uuidGenerator) {
    this.organizationPersistence = organizationPersistence;
    this.permissionPersistence = permissionPersistence;
    this.uuidGenerator = uuidGenerator;
  }

  public OrganizationRead createOrganization(final OrganizationCreateRequestBody organizationCreateRequestBody)
      throws IOException, ConfigNotFoundException {
    final String organizationName = organizationCreateRequestBody.getOrganizationName();
    final String email = organizationCreateRequestBody.getEmail();
    Organization organization = new Organization().withOrganizationId(uuidGenerator.get()).withName(organizationName).withEmail(email);
    organizationPersistence.createOrganization(organization);
    return buildOrganizationRead(organization);
  }

  public OrganizationRead updateOrganization(final OrganizationUpdateRequestBody organizationUpdateRequestBody)
      throws IOException, ConfigNotFoundException {
    final UUID organizationId = organizationUpdateRequestBody.getOrganizationId();
    Organization organization = organizationPersistence.getOrganization(organizationId)
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.ORGANIZATION, organizationId));
    boolean hasChanged = false;
    if (!organization.getName().equals(organizationUpdateRequestBody.getOrganizationName())) {
      organization.setName(organizationUpdateRequestBody.getOrganizationName());
      hasChanged = true;
    }
    if (hasChanged) {
      organizationPersistence.updateOrganization(organization);
    }
    return buildOrganizationRead(organization);
  }

  public OrganizationRead getOrganization(final OrganizationIdRequestBody organizationIdRequestBody) throws IOException, ConfigNotFoundException {
    final UUID organizationId = organizationIdRequestBody.getOrganizationId();
    Optional<Organization> organization = organizationPersistence.getOrganization(organizationId);
    if (organization.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.ORGANIZATION, organizationId);
    }
    return buildOrganizationRead(organization.get());
  }

  private OrganizationRead buildOrganizationRead(final Organization organization) {
    return new OrganizationRead()
        .organizationId(organization.getOrganizationId())
        .organizationName(organization.getName())
        .email(organization.getEmail());
  }

}
