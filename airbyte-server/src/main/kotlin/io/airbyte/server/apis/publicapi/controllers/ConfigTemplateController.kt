/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.ConfigTemplateApi
import io.airbyte.publicApi.server.generated.models.ConfigTemplateCreateRequestBody
import io.airbyte.publicApi.server.generated.models.ConfigTemplateCreateResponse
import io.airbyte.publicApi.server.generated.models.ConfigTemplateListRequest
import io.airbyte.publicApi.server.generated.models.ConfigTemplateRead
import io.airbyte.publicApi.server.generated.models.ConfigTemplateRequestBody
import io.airbyte.publicApi.server.generated.models.ConfigTemplateUpdateRequestBody
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpAttributes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.scheduling.annotation.ExecuteOn
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
open class ConfigTemplateController(
  private val currentUserService: CurrentUserService,
  private val configTemplateService: ConfigTemplateService,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val trackingHelper: TrackingHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
) : ConfigTemplateApi {
  private val logger = KotlinLogging.logger {}

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun createConfigTemplate(configTemplateCreateRequestBody: ConfigTemplateCreateRequestBody): Response =
    wrap {
      val userId: UUID = currentUserService.currentUser.userId
      val organizationId = configTemplateCreateRequestBody.organizationId

      apiAuthorizationHelper.isUserOrganizationAdminOrThrow(userId, organizationId)

      licenseEntitlementChecker.ensureEntitled(
        organizationId,
        Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
      )

      apiAuthorizationHelper.isUserOrganizationAdminOrThrow(userId, organizationId)
      val configTemplate =
        configTemplateService.createTemplate(
          OrganizationId(configTemplateCreateRequestBody.organizationId),
          ActorDefinitionId(configTemplateCreateRequestBody.actorDefinitionId),
          configTemplateCreateRequestBody.partialDefaultConfig,
          configTemplateCreateRequestBody.partialUserConfigSpec,
        )

      configTemplate.toPublicApiModel().ok()
    }

  override fun getConfigTemplate(configTemplateRequestBody: ConfigTemplateRequestBody): Response =
    wrap {
      configTemplateService.getConfigTemplate(configTemplateRequestBody.configTemplateId).toPublicApiModel().ok()
    }

  override fun listConfigTemplates(configTemplateListRequest: ConfigTemplateListRequest): Response =
    wrap {
      val userId: UUID = currentUserService.currentUser.userId
      apiAuthorizationHelper.isUserOrganizationAdminOrThrow(userId, configTemplateListRequest.organizationId)
      configTemplateService
        .listConfigTemplatesForOrganization(OrganizationId(configTemplateListRequest.organizationId))
        .map { it.toPublicApiModel() }
        .ok()
    }

  override fun updateConfigTemplate(configTemplateUpdateRequestBody: ConfigTemplateUpdateRequestBody): Response =
    wrap {
      val userId: UUID = currentUserService.currentUser.userId
      val organizationId = configTemplateUpdateRequestBody.organizationId

      apiAuthorizationHelper.isUserOrganizationAdminOrThrow(userId, organizationId)

      licenseEntitlementChecker.ensureEntitled(
        organizationId,
        Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
      )

      configTemplateService
        .updateTemplate(
          configTemplateId = configTemplateUpdateRequestBody.configTemplateId,
          organizationId = OrganizationId(organizationId),
          partialDefaultConfig = configTemplateUpdateRequestBody.partialDefaultConfig,
          userConfigSpec = configTemplateUpdateRequestBody.partialUserConfigSpec,
        ).toPublicApiModel()
        .ok()
    }

  private fun ConfigTemplate.toApiModel(): ConfigTemplateCreateResponse {
    val configTemplateCreateResponse = ConfigTemplateCreateResponse(id)
    return configTemplateCreateResponse
  }

  // wrap controller endpoints in common functionality: segment tracking, error conversion, etc.
  private fun wrap(block: () -> Response): Response {
    val currentRequest = ServerRequestContext.currentRequest<Any>().get()
    val template = currentRequest.attributes.get(HttpAttributes.URI_TEMPLATE.toString(), String::class.java).orElse(currentRequest.path)
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

  private fun ConfigTemplateWithActorDetails.toPublicApiModel(): ConfigTemplateRead =
    ConfigTemplateRead(
      id = this.configTemplate.id,
      sourceDefinitionId = this.configTemplate.actorDefinitionId,
      configTemplateSpec = this.configTemplate.partialDefaultConfig,
      icon = this.actorIcon,
      name = this.actorName,
    )
}
