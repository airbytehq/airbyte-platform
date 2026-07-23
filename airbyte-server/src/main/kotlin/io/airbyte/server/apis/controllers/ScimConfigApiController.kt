/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.server.generated.apis.ScimConfigApi
import io.airbyte.api.server.generated.models.EnableScimRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.ScimConfigResponse
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimOrganizationNotFoundException
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.airbyte.api.server.generated.models.ScimConfigStatus as ApiScimConfigStatus
import io.airbyte.api.server.generated.models.ScimIdpProvider as ApiScimIdpProvider
import io.airbyte.domain.models.scim.ScimConfigurationStatus as DomainScimConfigurationStatus
import io.airbyte.domain.models.scim.ScimIdpProvider as DomainScimIdpProvider

private const val SCIM_OPERATION_FAILURE_MESSAGE = "SCIM configuration operation failed"

@Controller
open class ScimConfigApiController(
  private val scimConfigurationService: ScimConfigurationService,
  private val currentUserService: CurrentUserService,
  private val webUrlHelper: WebUrlHelper,
) : ScimConfigApi {
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SCIM)
  override fun getScimConfig(organizationIdRequestBody: OrganizationIdRequestBody): ScimConfigResponse =
    executeScim {
      scimConfigurationService
        .getConfiguration(OrganizationId(organizationIdRequestBody.organizationId))
        .toApi()
    }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SCIM)
  override fun enableScim(enableScimRequestBody: EnableScimRequestBody): ScimConfigResponse =
    executeScim {
      scimConfigurationService
        .enable(
          organizationId = OrganizationId(enableScimRequestBody.organizationId),
          idpProvider = enableScimRequestBody.idpProvider.toDomain(),
          userId = currentUserId(),
        ).toApi()
    }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SCIM)
  override fun rotateScimToken(organizationIdRequestBody: OrganizationIdRequestBody): ScimConfigResponse =
    executeScim {
      scimConfigurationService
        .rotateToken(
          organizationId = OrganizationId(organizationIdRequestBody.organizationId),
          userId = currentUserId(),
        ).toApi()
    }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SCIM)
  @Status(HttpStatus.NO_CONTENT)
  override fun disableScim(organizationIdRequestBody: OrganizationIdRequestBody) {
    executeScim {
      scimConfigurationService.disable(
        organizationId = OrganizationId(organizationIdRequestBody.organizationId),
        userId = currentUserId(),
      )
    }
  }

  private fun currentUserId(): UserId = UserId(currentUserService.getCurrentUser().userId)

  private fun ScimConfigurationRead.toApi(): ScimConfigResponse =
    ScimConfigResponse(
      status =
        when (status) {
          DomainScimConfigurationStatus.NOT_CONFIGURED -> ApiScimConfigStatus.NOT_CONFIGURED
          DomainScimConfigurationStatus.ENABLED -> ApiScimConfigStatus.ENABLED
          DomainScimConfigurationStatus.DISABLED -> ApiScimConfigStatus.DISABLED
        },
      scimBaseUrl = "${webUrlHelper.baseUrl}/scim/v2",
      idpProvider = idpProvider?.toApi(),
      createdAt = createdAt?.toEpochSecond(),
      updatedAt = updatedAt?.toEpochSecond(),
      token = token,
    )

  private fun ApiScimIdpProvider.toDomain(): DomainScimIdpProvider =
    when (this) {
      ApiScimIdpProvider.OKTA -> DomainScimIdpProvider.OKTA
      ApiScimIdpProvider.MICROSOFT_ENTRA_ID -> DomainScimIdpProvider.MICROSOFT_ENTRA_ID
    }

  private fun DomainScimIdpProvider.toApi(): ApiScimIdpProvider =
    when (this) {
      DomainScimIdpProvider.OKTA -> ApiScimIdpProvider.OKTA
      DomainScimIdpProvider.MICROSOFT_ENTRA_ID -> ApiScimIdpProvider.MICROSOFT_ENTRA_ID
    }

  private fun <T> executeScim(call: () -> T): T =
    try {
      call()
    } catch (e: ScimAccessDeniedException) {
      throw OperationNotAllowedException(e.message, e)
    } catch (e: ScimConfigurationConflictException) {
      throw ConflictException(e.message, e)
    } catch (e: ScimOrganizationNotFoundException) {
      throw IdNotFoundKnownException(e.message, e.organizationId.toString(), e)
    } catch (_: Exception) {
      throw RuntimeException(SCIM_OPERATION_FAILURE_MESSAGE)
    }
}
