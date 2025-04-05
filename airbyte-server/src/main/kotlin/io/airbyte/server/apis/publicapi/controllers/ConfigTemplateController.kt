/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.ConfigTemplateApi
import io.airbyte.publicApi.server.generated.models.ConfigTemplateRead
import io.airbyte.publicApi.server.generated.models.ConfigTemplateRequestBody
import io.airbyte.publicApi.server.generated.models.ListConfigTemplatesRequestBody
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpAttributes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConfigTemplateController(
  private val currentUserService: CurrentUserService,
  private val configTemplateService: ConfigTemplateService,
  private val trackingHelper: TrackingHelper,
) : ConfigTemplateApi {
  private val logger = KotlinLogging.logger {}

  override fun getConfigTemplate(configTemplateRequestBody: ConfigTemplateRequestBody): Response =
    wrap {
      configTemplateService.getConfigTemplate(configTemplateRequestBody.configTemplateId).toPublicApiModel().ok()
    }

  override fun listConfigTemplates(listConfigTemplatesRequestBody: ListConfigTemplatesRequestBody): Response =
    wrap {
      val userId: UUID = currentUserService.currentUser.userId
      configTemplateService
        .listConfigTemplatesForOrganization(OrganizationId(listConfigTemplatesRequestBody.organizationId))
        .map { it.toPublicApiModel() }
        .ok()
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
