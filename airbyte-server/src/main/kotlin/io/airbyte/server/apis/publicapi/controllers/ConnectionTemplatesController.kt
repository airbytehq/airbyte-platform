/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Cron
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.PublicConnectionTemplatesApi
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConnectionTemplateCreateRequestBody
import io.airbyte.publicApi.server.generated.models.ConnectionTemplateCreateResponse
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType
import io.airbyte.publicApi.server.generated.models.NonBreakingChangesPreference
import io.airbyte.publicApi.server.generated.models.ResourceRequirements
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper.normalizeCronExpression
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper.validateCronConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.BasicHttpAttributes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConnectionTemplatesController(
  private val currentUserService: CurrentUserService,
  private val trackingHelper: TrackingHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
  private val connectionTemplateService: ConnectionTemplateService,
) : PublicConnectionTemplatesApi {
  private val logger = KotlinLogging.logger {}

  companion object {
    val DEFAULT_CRON_SCHEDULE = ScheduleData().withCron(Cron().withCronExpression("0 0 * * * ?").withCronTimeZone("UTC"))
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  override fun publicCreateConnectionTemplate(connectionTemplateCreateRequestBody: ConnectionTemplateCreateRequestBody): Response =
    wrap {
      createConnectionTemplate(connectionTemplateCreateRequestBody).ok()
    }

  @VisibleForTesting
  fun createConnectionTemplate(connectionTemplateCreateRequestBody: ConnectionTemplateCreateRequestBody): ConnectionTemplateCreateResponse {
    // FIXME: we should optionally create the destinations and connections in existing workspaces https://github.com/airbytehq/airbyte-internal-issues/issues/12813
    val organizationId = OrganizationId(connectionTemplateCreateRequestBody.organizationId)

    licenseEntitlementChecker.ensureEntitled(
      organizationId.value,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )
    licenseEntitlementChecker.ensureEntitled(
      organizationId.value,
      Entitlement.DESTINATION_CONNECTOR,
      connectionTemplateCreateRequestBody.destinationActorDefinitionId,
    )

    val namespaceDefinitionType = convertNamespaceDefinitionType(connectionTemplateCreateRequestBody.namespaceDefinitionType)
    val connectionTemplate =
      connectionTemplateService.createTemplate(
        organizationId,
        connectionTemplateCreateRequestBody.destinationName,
        ActorDefinitionId(connectionTemplateCreateRequestBody.destinationActorDefinitionId),
        connectionTemplateCreateRequestBody.destinationConfiguration,
        namespaceDefinitionType,
        connectionTemplateCreateRequestBody.namespaceFormat,
        connectionTemplateCreateRequestBody.prefix,
        convertScheduleData(connectionTemplateCreateRequestBody.schedule),
        convertResourceRequirements(connectionTemplateCreateRequestBody.resourceRequirements),
        convertNonBreakingChangesPreference(connectionTemplateCreateRequestBody.nonBreakingChangesPreference),
        connectionTemplateCreateRequestBody.syncOnCreate ?: true,
      )

    return ConnectionTemplateCreateResponse(connectionTemplate.id)
  }

  private fun convertNonBreakingChangesPreference(
    nonBreakingChangesPreference: NonBreakingChangesPreference?,
  ): StandardSync.NonBreakingChangesPreference {
    if (nonBreakingChangesPreference == null) {
      return StandardSync.NonBreakingChangesPreference.IGNORE
    } else {
      return when (nonBreakingChangesPreference) {
        NonBreakingChangesPreference.IGNORE -> StandardSync.NonBreakingChangesPreference.IGNORE
        NonBreakingChangesPreference.DISABLE -> StandardSync.NonBreakingChangesPreference.DISABLE
        NonBreakingChangesPreference.PROPAGATE_COLUMNS -> StandardSync.NonBreakingChangesPreference.PROPAGATE_COLUMNS
        NonBreakingChangesPreference.PROPAGATE_FULLY -> StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY
      }
    }
  }

  private fun convertResourceRequirements(resourceRequirements: ResourceRequirements?): io.airbyte.config.ResourceRequirements? {
    if (resourceRequirements == null) {
      return null
    } else {
      return io.airbyte.config
        .ResourceRequirements()
        .withCpuLimit(resourceRequirements.cpuLimit)
        .withCpuRequest(resourceRequirements.cpuRequest)
        .withMemoryLimit(resourceRequirements.memoryLimit)
        .withMemoryRequest(resourceRequirements.memoryRequest)
        .withEphemeralStorageRequest(resourceRequirements.ephemeralStorageRequest)
        .withEphemeralStorageLimit(resourceRequirements.ephemeralStorageLimit)
    }
  }

  private fun convertScheduleData(scheduleData: AirbyteApiConnectionSchedule?): ScheduleData? =
    if (scheduleData == null) {
      DEFAULT_CRON_SCHEDULE
    } else if (scheduleData.scheduleType == ScheduleTypeEnum.MANUAL) {
      null
    } else {
      validateCronConfiguration(scheduleData)
      ScheduleData().withCron(Cron().withCronExpression(normalizeCronExpression(scheduleData)!!.cronExpression).withCronTimeZone("UTC"))
    }

  private fun convertNamespaceDefinitionType(
    namespaceDefinitionType: NamespaceDefinitionType?,
  ): io.airbyte.config.JobSyncConfig.NamespaceDefinitionType =
    when (namespaceDefinitionType) {
      NamespaceDefinitionType.SOURCE -> io.airbyte.config.JobSyncConfig.NamespaceDefinitionType.SOURCE
      NamespaceDefinitionType.DESTINATION -> io.airbyte.config.JobSyncConfig.NamespaceDefinitionType.DESTINATION
      NamespaceDefinitionType.CUSTOMFORMAT -> io.airbyte.config.JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT
      null -> io.airbyte.config.JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT
    }

  // wrap controller endpoints in common functionality: segment tracking, error conversion, etc.
  private fun wrap(block: () -> Response): Response {
    val currentRequest = ServerRequestContext.currentRequest<Any>().get()
    val template = BasicHttpAttributes.getUriTemplate(currentRequest).orElse(currentRequest.path)
    val method = currentRequest.method.name

    val userId: UUID = currentUserService.getCurrentUser().userId

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
