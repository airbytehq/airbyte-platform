/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody;
import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationReadList;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OrganizationHandler for handling organization resource related operation.
 * <p>
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class OrganizationsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OrganizationsHandler.class);
  private final PermissionPersistence permissionPersistence;
  private final OrganizationPersistence organizationPersistence;

  private final Supplier<UUID> uuidGenerator;

  @Inject
  public OrganizationsHandler(final OrganizationPersistence organizationPersistence,
                              final PermissionPersistence permissionPersistence,
                              @Named("uuidGenerator") final Supplier<UUID> uuidGenerator) {
    this.organizationPersistence = organizationPersistence;
    this.permissionPersistence = permissionPersistence;
    this.uuidGenerator = uuidGenerator;
  }

  private static OrganizationRead buildOrganizationRead(final Organization organization) {
    return new OrganizationRead()
        .organizationId(organization.getOrganizationId())
        .organizationName(organization.getName())
        .email(organization.getEmail())
        .pba(organization.getPba())
        .orgLevelBilling(organization.getOrgLevelBilling())
        .ssoRealm(organization.getSsoRealm());
  }

  public OrganizationRead createOrganization(final OrganizationCreateRequestBody organizationCreateRequestBody)
      throws IOException {
    final String organizationName = organizationCreateRequestBody.getOrganizationName();
    final String email = organizationCreateRequestBody.getEmail();
    final UUID userId = organizationCreateRequestBody.getUserId();
    final UUID orgId = uuidGenerator.get();
    final Boolean pba = organizationCreateRequestBody.getPba() != null && organizationCreateRequestBody.getPba();
    final Boolean orgLevelBilling = organizationCreateRequestBody.getOrgLevelBilling() != null && organizationCreateRequestBody.getOrgLevelBilling();
    final Organization organization = new Organization()
        .withOrganizationId(orgId)
        .withName(organizationName)
        .withEmail(email)
        .withUserId(userId)
        .withPba(pba)
        .withOrgLevelBilling(orgLevelBilling);
    organizationPersistence.createOrganization(organization);
    // Also create an OrgAdmin permission.
    final Permission orgAdminPermission = new Permission()
        .withPermissionId(uuidGenerator.get())
        .withUserId(userId)
        .withOrganizationId(orgId)
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN);
    permissionPersistence.writePermission(orgAdminPermission);
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
    if (organizationUpdateRequestBody.getPba() != null && !organization.getPba().equals(organizationUpdateRequestBody.getPba())) {
      organization.setPba(organizationUpdateRequestBody.getPba());
      hasChanged = true;
    }
    if (organizationUpdateRequestBody.getOrgLevelBilling() != null && !organization.getOrgLevelBilling()
        .equals(organizationUpdateRequestBody.getOrgLevelBilling())) {
      organization.setOrgLevelBilling(organizationUpdateRequestBody.getOrgLevelBilling());
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
