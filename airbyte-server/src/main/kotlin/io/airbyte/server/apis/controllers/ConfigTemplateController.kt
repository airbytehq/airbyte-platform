/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ConfigTemplateApi
import io.airbyte.api.model.generated.ConfigTemplateList
import io.airbyte.api.model.generated.ConfigTemplateListItem
import io.airbyte.api.model.generated.ConfigTemplateRead
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.api.model.generated.ListConfigTemplatesRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.persistence.job.WorkspaceHelper
import io.micronaut.http.annotation.Controller

@Controller
open class ConfigTemplateController(
  private val configTemplateService: ConfigTemplateService,
  val workspaceHelper: WorkspaceHelper,
  val licenseEntitlementChecker: LicenseEntitlementChecker,
) : ConfigTemplateApi {
  @RequiresIntent(Intent.ViewConfigTemplates)
  override fun getConfigTemplate(req: ConfigTemplateRequestBody): ConfigTemplateRead {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(req.workspaceId)

    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )

    return configTemplateService
      .getConfigTemplate(req.configTemplateId)
      .toApiModel()
  }

  @RequiresIntent(Intent.ViewConfigTemplates)
  override fun listConfigTemplates(req: ListConfigTemplatesRequestBody): ConfigTemplateList {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(req.workspaceId)

    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )

    return ConfigTemplateList()
      .configTemplates(
        configTemplateService
          .listConfigTemplatesForOrganization(OrganizationId(organizationId))
          .map { it.toListItem() },
      )
  }
}

private fun ConfigTemplateWithActorDetails.toApiModel() =
  ConfigTemplateRead()
    .sourceDefinitionId(this.configTemplate.actorDefinitionId)
    .configTemplateSpec(this.configTemplate.partialDefaultConfig)
    .icon(this.actorIcon)
    .name(this.actorName)
    .id(this.configTemplate.id)

private fun ConfigTemplateWithActorDetails.toListItem() =
  ConfigTemplateListItem()
    .id(this.configTemplate.id)
    .icon(this.actorIcon)
    .name(this.actorName)
