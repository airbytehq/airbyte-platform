/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody;
import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationReadList;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.commons.server.errors.ConflictException;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.data.services.PermissionRedundantException;
import io.airbyte.data.services.PermissionService;
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jooq.tools.StringUtils;

/**
 * OrganizationHandler for handling organization resource related operation.
 * <p>
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class OrganizationsHandler {

  private final PermissionService permissionService;
  private final OrganizationPersistence organizationPersistence;

  private final Supplier<UUID> uuidGenerator;

  @Inject
  public OrganizationsHandler(final OrganizationPersistence organizationPersistence,
                              final PermissionService permissionService,
                              @Named("uuidGenerator") final Supplier<UUID> uuidGenerator) {
    this.organizationPersistence = organizationPersistence;
    this.permissionService = permissionService;
    this.uuidGenerator = uuidGenerator;
  }

  private static OrganizationRead buildOrganizationRead(final Organization organization) {
    return new OrganizationRead()
        .organizationId(organization.getOrganizationId())
        .organizationName(organization.getName())
        .email(organization.getEmail())
        .ssoRealm(organization.getSsoRealm());
  }

  public OrganizationRead createOrganization(final OrganizationCreateRequestBody organizationCreateRequestBody)
      throws IOException {
    final String organizationName = organizationCreateRequestBody.getOrganizationName();
    final String email = organizationCreateRequestBody.getEmail();
    final UUID userId = organizationCreateRequestBody.getUserId();
    final UUID orgId = uuidGenerator.get();
    final Organization organization = new Organization()
        .withOrganizationId(orgId)
        .withName(organizationName)
        .withEmail(email)
        .withUserId(userId);
    organizationPersistence.createOrganization(organization);

    try {
      // Also create an OrgAdmin permission.
      permissionService.createPermission(new Permission()
          .withPermissionId(uuidGenerator.get())
          .withUserId(userId)
          .withOrganizationId(orgId)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN));
    } catch (final PermissionRedundantException e) {
      throw new ConflictException(e.getMessage(), e);
    }
    return buildOrganizationRead(organization);
  }

  public OrganizationRead updateOrganization(final OrganizationUpdateRequestBody organizationUpdateRequestBody)
      throws IOException, ConfigNotFoundException {
    final UUID organizationId = organizationUpdateRequestBody.getOrganizationId();
    final Organization organization = organizationPersistence.getOrganization(organizationId)
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.ORGANIZATION, organizationId));
    boolean hasChanged = false;
    if (!organization.getName().equals(organizationUpdateRequestBody.getOrganizationName())) {
      organization.setName(organizationUpdateRequestBody.getOrganizationName());
      hasChanged = true;
    }
    if (organizationUpdateRequestBody.getEmail() != null && !organizationUpdateRequestBody.getEmail()
        .equals(organization.getEmail())) {
      organization.setEmail(organizationUpdateRequestBody.getEmail());
      hasChanged = true;
    }
    if (hasChanged) {
      organizationPersistence.updateOrganization(organization);
    }
    return buildOrganizationRead(organization);
  }

  public OrganizationRead getOrganization(final OrganizationIdRequestBody organizationIdRequestBody) throws IOException, ConfigNotFoundException {
    final UUID organizationId = organizationIdRequestBody.getOrganizationId();
    final Optional<Organization> organization = organizationPersistence.getOrganization(organizationId);
    if (organization.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.ORGANIZATION, organizationId);
    }
    return buildOrganizationRead(organization.get());
  }

  public OrganizationReadList listOrganizationsByUser(final ListOrganizationsByUserRequestBody request) throws IOException {
    final Optional<String> nameContains = StringUtils.isBlank(request.getNameContains()) ? Optional.empty() : Optional.of(request.getNameContains());
    final List<OrganizationRead> organizationReadList;
    if (request.getPagination() != null) {
      organizationReadList = organizationPersistence
          .listOrganizationsByUserIdPaginated(
              new ResourcesByUserQueryPaginated(request.getUserId(),
                  false, request.getPagination().getPageSize(), request.getPagination().getRowOffset()),
              nameContains)
          .stream()
          .map(OrganizationsHandler::buildOrganizationRead)
          .collect(Collectors.toList());
    } else {
      organizationReadList = organizationPersistence
          .listOrganizationsByUserId(request.getUserId(), nameContains)
          .stream()
          .map(OrganizationsHandler::buildOrganizationRead)
          .collect(Collectors.toList());
    }
    return new OrganizationReadList().organizations(organizationReadList);
  }

}
