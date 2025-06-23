/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.config.ConfigSchema
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.tools.StringUtils
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

@Singleton
open class OrganizationsHandler(
  private val organizationPersistence: OrganizationPersistence,
  private val permissionHandler: PermissionHandler,
  @Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
  private val organizationPaymentConfigService: OrganizationPaymentConfigService,
) {
  companion object {
    private fun buildOrganizationRead(organization: Organization): OrganizationRead =
      OrganizationRead()
        .organizationId(organization.organizationId)
        .organizationName(organization.name)
        .email(organization.email)
        .ssoRealm(organization.ssoRealm)
  }

  @Throws(IOException::class, JsonValidationException::class, PermissionRedundantException::class)
  fun createOrganization(request: OrganizationCreateRequestBody): OrganizationRead {
    val organizationName = request.organizationName
    val email = request.email
    val userId = request.userId
    val orgId = uuidGenerator.get()
    val organization =
      Organization()
        .withOrganizationId(orgId)
        .withName(organizationName)
        .withEmail(email)
        .withUserId(userId)
    organizationPersistence.createOrganization(organization)

    // Also create an OrgAdmin permission.
    permissionHandler.createPermission(
      Permission()
        .withPermissionId(uuidGenerator.get())
        .withUserId(userId)
        .withOrganizationId(orgId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
    )

    organizationPaymentConfigService.saveDefaultPaymentConfig(organization.organizationId)
    return buildOrganizationRead(organization)
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun updateOrganization(request: OrganizationUpdateRequestBody): OrganizationRead {
    val organizationId = request.organizationId
    val organization =
      organizationPersistence
        .getOrganization(organizationId)
        .orElseThrow { ConfigNotFoundException(ConfigSchema.ORGANIZATION, organizationId) }

    var hasChanged = false
    if (organization.name != request.organizationName) {
      organization.name = request.organizationName
      hasChanged = true
    }

    if (request.email != null && request.email != organization.email) {
      organization.email = request.email
      hasChanged = true
    }

    if (hasChanged) {
      organizationPersistence.updateOrganization(organization)
    }

    return buildOrganizationRead(organization)
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getOrganization(request: OrganizationIdRequestBody): OrganizationRead {
    val organizationId = request.organizationId
    val organization =
      organizationPersistence
        .getOrganization(organizationId)
        .orElseThrow { ConfigNotFoundException(ConfigSchema.ORGANIZATION, organizationId) }

    return buildOrganizationRead(organization)
  }

  @Throws(IOException::class)
  fun listOrganizationsByUser(request: ListOrganizationsByUserRequestBody): OrganizationReadList {
    val nameContains =
      if (StringUtils.isBlank(request.nameContains)) {
        Optional.empty<String>()
      } else {
        Optional.of(request.nameContains)
      }

    val organizationReadList =
      if (request.pagination != null) {
        organizationPersistence
          .listOrganizationsByUserIdPaginated(
            ResourcesByUserQueryPaginated(
              request.userId,
              false,
              request.pagination.pageSize,
              request.pagination.rowOffset,
            ),
            nameContains,
          ).map { buildOrganizationRead(it) }
      } else {
        organizationPersistence
          .listOrganizationsByUserId(request.userId, nameContains)
          .map { buildOrganizationRead(it) }
      }

    return OrganizationReadList().organizations(organizationReadList)
  }
}
