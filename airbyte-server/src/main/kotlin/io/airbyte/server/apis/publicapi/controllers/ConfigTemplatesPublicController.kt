/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.problems.model.generated.ProblemLicenseEntitlementData
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.PublicConfigTemplatesApi
import io.airbyte.publicApi.server.generated.models.ConfigTemplateCreateRequestBody
import io.airbyte.publicApi.server.generated.models.ConfigTemplateCreateResponse
import io.airbyte.publicApi.server.generated.models.ConfigTemplateListItem
import io.airbyte.publicApi.server.generated.models.ConfigTemplateListResponse
import io.airbyte.publicApi.server.generated.models.ConfigTemplatePublicRead
import io.airbyte.publicApi.server.generated.models.ConfigTemplateUpdateRequestBody
import io.airbyte.publicApi.server.generated.models.ConfigTemplateUpdateResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.BasicHttpAttributes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConfigTemplatesPublicController(
  private val currentUserService: CurrentUserService,
  private val configTemplateService: ConfigTemplateService,
  private val trackingHelper: TrackingHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
  private val organizationsHandler: OrganizationsHandler,
) : PublicConfigTemplatesApi {
  private val logger = KotlinLogging.logger {}

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  override fun publicCreateConfigTemplate(configTemplateCreateRequestBody: ConfigTemplateCreateRequestBody): Response =
    wrap {
      createConfigTemplate(configTemplateCreateRequestBody).ok()
    }

  @VisibleForTesting
  fun createConfigTemplate(configTemplateCreateRequestBody: ConfigTemplateCreateRequestBody): ConfigTemplateCreateResponse {
    val organizationId = configTemplateCreateRequestBody.organizationId

    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )
    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.SOURCE_CONNECTOR,
      configTemplateCreateRequestBody.actorDefinitionId,
    )

    val configTemplate =
      configTemplateService.createTemplate(
        OrganizationId(configTemplateCreateRequestBody.organizationId),
        ActorDefinitionId(configTemplateCreateRequestBody.actorDefinitionId),
        configTemplateCreateRequestBody.partialDefaultConfig,
        configTemplateCreateRequestBody.partialUserConfigSpec,
      )

    return ConfigTemplateCreateResponse(id = configTemplate.configTemplate.id)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Secured(AuthRoleConstants.ORGANIZATION_READER)
  override fun publicGetConfigTemplate(configTemplateId: UUID): Response =
    wrap {
      getConfigTemplate(configTemplateId).ok()
    }

  @VisibleForTesting
  fun getConfigTemplate(configTemplateId: UUID): ConfigTemplatePublicRead {
    val user = currentUserService.currentUser
    val userId: UUID = user.userId

    ensureUserBelongsToAtLeastOneEntitledOrg(userId)

    val configTemplate = configTemplateService.getConfigTemplate(configTemplateId)
    return configTemplate.toApiModel()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Secured(AuthRoleConstants.ORGANIZATION_READER)
  override fun publicListConfigTemplate(organizationId: String): Response =
    wrap {
      listConfigTemplate(organizationId).ok()
    }

  @VisibleForTesting
  fun listConfigTemplate(organizationId: String): ConfigTemplateListResponse {
    licenseEntitlementChecker.checkEntitlements(
      UUID.fromString(organizationId),
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )

    val configTemplates =
      configTemplateService
        .listConfigTemplatesForOrganization(OrganizationId(UUID.fromString(organizationId)))
        .map { it.toListItemApiModel() }
    return ConfigTemplateListResponse(
      data = configTemplates,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  override fun publicUpdateConfigTemplate(
    configTemplateId: UUID,
    configTemplateUpdateRequestBody: ConfigTemplateUpdateRequestBody,
  ): Response =
    wrap {
      updateConfigTemplate(configTemplateId, configTemplateUpdateRequestBody).ok()
    }

  @VisibleForTesting
  fun updateConfigTemplate(
    configTemplateId: UUID,
    configTemplateUpdateRequestBody: ConfigTemplateUpdateRequestBody,
  ): ConfigTemplateUpdateResponse {
    val organizationId = configTemplateUpdateRequestBody.organizationId

    licenseEntitlementChecker.ensureEntitled(
      organizationId,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )

    val updated =
      configTemplateService
        .updateTemplate(
          configTemplateId = configTemplateId,
          organizationId = OrganizationId(organizationId),
          partialDefaultConfig = configTemplateUpdateRequestBody.partialDefaultConfig,
          userConfigSpec = configTemplateUpdateRequestBody.partialUserConfigSpec,
        )

    return ConfigTemplateUpdateResponse(id = updated.configTemplate.id)
  }

  private fun ensureUserBelongsToAtLeastOneEntitledOrg(userId: UUID) {
    val anyOrganizationIsEntitled =
      organizationsHandler.listOrganizationsByUser(ListOrganizationsByUserRequestBody().userId(userId)).organizations.any {
        licenseEntitlementChecker.checkEntitlements(
          it.organizationId,
          Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
        )
      }

    if (!anyOrganizationIsEntitled) {
      throw LicenseEntitlementProblem(
        ProblemLicenseEntitlementData()
          .entitlement(Entitlement.CONFIG_TEMPLATE_ENDPOINTS.name),
      )
    }
  }

  private fun ConfigTemplateWithActorDetails.toApiModel(): ConfigTemplatePublicRead =
    ConfigTemplatePublicRead(
      id = this.configTemplate.id,
      name = actorName,
      icon = this.actorIcon,
      sourceDefinitionId = this.configTemplate.actorDefinitionId,
      configTemplateSpec = this.configTemplate.userConfigSpec,
      partialDefaultConfig = this.configTemplate.partialDefaultConfig,
    )

  private fun ConfigTemplateWithActorDetails.toListItemApiModel(): ConfigTemplateListItem =
    ConfigTemplateListItem(
      id = this.configTemplate.id,
      name = actorName,
      icon = this.actorIcon,
    )

  // wrap controller endpoints in common functionality: segment tracking, error conversion, etc.
  private fun wrap(block: () -> Response): Response {
    val currentRequest = ServerRequestContext.currentRequest<Any>().get()
    val template = BasicHttpAttributes.getUriTemplate(currentRequest).orElse(currentRequest.path)
    val method = currentRequest.method.name

    val userId: UUID = currentUserService.currentUser.userId

    val res: Response =
      trackingHelper.callWithTracker({
        try {
          return@callWithTracker block()
        } catch (e: Exception) {
          logger.error(e) { "Failed to call `${currentRequest.path}`" }
          ConfigClientErrorHandler.handleError(e)
          // handleError() above should always throw an exception,
          // but if it doesn't, return an unknown server error.
          return@callWithTracker Response.serverError().build()
        }
      }, template, method, userId)
    trackingHelper.trackSuccess(template, method, userId)
    return res
  }

  private fun <T> T.ok() = Response.status(Response.Status.OK.statusCode).entity(this).build()
}
