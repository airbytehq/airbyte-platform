/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.PublicConfigTemplatesApi
import io.airbyte.publicApi.server.generated.models.ConfigTemplateCreateRequestBody
import io.airbyte.publicApi.server.generated.models.ConfigTemplateList
import io.airbyte.publicApi.server.generated.models.ConfigTemplateListItem
import io.airbyte.publicApi.server.generated.models.ConfigTemplateUpdateRequestBody
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.APPLICATIONS_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpAttributes
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
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val trackingHelper: TrackingHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
) : PublicConfigTemplatesApi {
  private val logger = KotlinLogging.logger {}

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateConfigTemplate(configTemplateCreateRequestBody: ConfigTemplateCreateRequestBody): Response =
    wrap {
      logger.info { "Creating config template from wrap" }
      val userId: UUID = currentUserService.currentUser.userId
      val organizationId = configTemplateCreateRequestBody.organizationId

      logger.info { "UserId: $userId" }
      logger.info {
        "Creating config template with organizationId: $organizationId and actorDefinitionId: ${configTemplateCreateRequestBody.actorDefinitionId}"
      }

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

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetConfigTemplate(configTemplateId: UUID): Response =
    wrap {
      configTemplateService.getConfigTemplate(configTemplateId).toPublicApiModel().ok()
    }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListConfigTemplate(): Response =
    wrap {
      logger.info { "Listing config templates from wrap" }
      val user: AuthenticatedUser = currentUserService.currentUser
      // process and monitor the request
      val configTemplates =
        trackingHelper.callWithTracker(
          {
            configTemplateService
              .listConfigTemplatesForOrganization(OrganizationId(UUID.fromString("00000000-0000-0000-0000-000000000000")))
              .map { it.toPublicApiModel() }
          },
          APPLICATIONS_PATH,
          GET,
          user.userId,
        )
      ConfigTemplateList(
        configTemplates = configTemplates,
      ).ok()
    }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicUpdateConfigTemplate(
    configTemplateId: UUID,
    configTemplateUpdateRequestBody: ConfigTemplateUpdateRequestBody,
  ): Response =
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
          configTemplateId = configTemplateId,
          organizationId = OrganizationId(organizationId),
          partialDefaultConfig = configTemplateUpdateRequestBody.partialDefaultConfig,
          userConfigSpec = configTemplateUpdateRequestBody.partialUserConfigSpec,
        ).toPublicApiModel()
        .ok()
    }

  private fun ConfigTemplateWithActorDetails.toPublicApiModel(): ConfigTemplateListItem =
    ConfigTemplateListItem(
      id = this.configTemplate.id,
      name = actorName,
      icon = this.actorIcon,
    )

  // wrap controller endpoints in common functionality: segment tracking, error conversion, etc.
  private fun wrap(block: () -> Response): Response {
    logger.info("wrapping request")
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
}
