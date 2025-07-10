/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers
import io.airbyte.api.generated.PartialUserConfigsApi
import io.airbyte.api.model.generated.ConfigTemplateRead
import io.airbyte.api.model.generated.ListPartialUserConfigsRequest
import io.airbyte.api.model.generated.PartialUserConfigCreate
import io.airbyte.api.model.generated.PartialUserConfigListItem
import io.airbyte.api.model.generated.PartialUserConfigRead
import io.airbyte.api.model.generated.PartialUserConfigReadList
import io.airbyte.api.model.generated.PartialUserConfigRequestBody
import io.airbyte.api.model.generated.PartialUserConfigUpdate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.problems.throwable.generated.EmbeddedEndpointMovedProblem
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.config.PartialUserConfigWithFullDetails
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.impls.data.mappers.objectMapper
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseSonarServer
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.server.handlers.PartialUserConfigHandler
import io.airbyte.server.helpers.ConfigTemplateAdvancedAuthHelper
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import java.util.UUID

@RequiresIntent(Intent.ViewAndEditPartialConfigs)
@Controller("/api/v1/partial_user_configs")
class PartialUserConfigController(
  private val partialUserConfigHandler: PartialUserConfigHandler,
  private val partialUserConfigService: PartialUserConfigService,
  private val featureFlagClient: FeatureFlagClient,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
  private val workspaceHelper: WorkspaceHelper,
) : PartialUserConfigsApi {
  @Post("/list")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPartialUserConfigs(
    @Body listPartialUserConfigRequestBody: ListPartialUserConfigsRequest,
  ): PartialUserConfigReadList {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(listPartialUserConfigRequestBody.workspaceId)

    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )
    return partialUserConfigService.listPartialUserConfigs(listPartialUserConfigRequestBody.workspaceId).toApiModel()
  }

  @Post("/create")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createPartialUserConfig(
    @Body partialUserConfigCreate: PartialUserConfigCreate,
  ): SourceRead {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(partialUserConfigCreate.workspaceId)

    throwIfSonarServerEnabled(workspaceHelper.getOrganizationForWorkspace(organizationId))
    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )
    return partialUserConfigHandler.createSourceFromPartialConfig(
      partialUserConfigCreate.toConfigModel(),
      partialUserConfigCreate.connectionConfiguration,
    )
  }

  @Post("/update")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updatePartialUserConfig(
    @Body partialUserConfigUpdate: PartialUserConfigUpdate,
  ): SourceRead {
    throwIfSonarServerEnabled(workspaceHelper.getOrganizationForWorkspace(partialUserConfigUpdate.workspaceId))
    return partialUserConfigHandler.updateSourceFromPartialConfig(
      partialUserConfigUpdate.toConfigModel(),
      partialUserConfigUpdate.connectionConfiguration,
    )
  }

  @Post("/get")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getPartialUserConfig(
    @Body partialUserConfigRequestBody: PartialUserConfigRequestBody,
  ): PartialUserConfigRead {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(partialUserConfigRequestBody.workspaceId)

    throwIfSonarServerEnabled(workspaceHelper.getOrganizationForWorkspace(organizationId))
    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )
    return partialUserConfigHandler.getPartialUserConfig(partialUserConfigRequestBody.partialUserConfigId).toApiModel()
  }

  @Post("/delete")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deletePartialUserConfig(
    @Body partialUserConfigIdRequestBody: PartialUserConfigRequestBody,
  ) {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(partialUserConfigIdRequestBody.workspaceId)

    throwIfSonarServerEnabled(workspaceHelper.getOrganizationForWorkspace(organizationId))
    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )

    return partialUserConfigHandler.deletePartialUserConfig(partialUserConfigIdRequestBody.partialUserConfigId)
  }

  private fun PartialUserConfigCreate.toConfigModel(): PartialUserConfig =
    PartialUserConfig(
      id = UUID.randomUUID(),
      workspaceId = this.workspaceId,
      configTemplateId = this.configTemplateId,
    )

  private fun PartialUserConfigUpdate.toConfigModel(): PartialUserConfig {
    val existingPartialUserConfig = partialUserConfigService.getPartialUserConfig(this.partialUserConfigId)

    return PartialUserConfig(
      id = partialUserConfigId,
      workspaceId = existingPartialUserConfig.partialUserConfig.workspaceId,
      configTemplateId = existingPartialUserConfig.partialUserConfig.configTemplateId,
    )
  }

  private fun List<PartialUserConfigWithActorDetails>.toApiModel(): PartialUserConfigReadList {
    val items =
      this.map { partialUserConfig ->
        PartialUserConfigListItem()
          .partialUserConfigId(partialUserConfig.partialUserConfig.id)
          .configTemplateIcon(partialUserConfig.actorIcon)
          .configTemplateName(partialUserConfig.actorName)
          .configTemplateId(partialUserConfig.configTemplateId)
      }
    return PartialUserConfigReadList().partialUserConfigs(items)
  }

  private fun PartialUserConfigWithFullDetails.toApiModel(): PartialUserConfigRead {
    val partialUserConfig =
      PartialUserConfigRead()
        .id(
          this.partialUserConfig.id,
        ).actorId(this.partialUserConfig.actorId)
        .connectionConfiguration(this.connectionConfiguration)
        .configTemplate(
          ConfigTemplateRead()
            .id(
              this.configTemplate.id,
            ).configTemplateSpec(
              this.configTemplate.userConfigSpec.let {
                objectMapper.valueToTree(it)
              },
            ).icon(this.actorIcon)
            .name(this.actorName)
            .sourceDefinitionId(this.configTemplate.actorDefinitionId),
        )

    if (this.configTemplate.advancedAuth != null) {
      partialUserConfig.configTemplate.advancedAuth(
        ConfigTemplateAdvancedAuthHelper.mapAdvancedAuth(this.configTemplate.advancedAuth!!),
      )
      partialUserConfig.configTemplate.advancedAuthGlobalCredentialsAvailable(
        this.configTemplate.advancedAuthGlobalCredentialsAvailable,
      )
    }

    return partialUserConfig
  }

  private fun throwIfSonarServerEnabled(organizationId: UUID) {
    if (featureFlagClient.boolVariation(UseSonarServer, Organization(organizationId))) {
      throw EmbeddedEndpointMovedProblem()
    }
  }
}
