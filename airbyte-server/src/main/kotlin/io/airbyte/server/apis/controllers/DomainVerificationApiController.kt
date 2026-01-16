/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.apis.DomainVerificationsApi
import io.airbyte.api.server.generated.models.DomainVerificationCreateRequestBody
import io.airbyte.api.server.generated.models.DomainVerificationIdRequestBody
import io.airbyte.api.server.generated.models.DomainVerificationListResponse
import io.airbyte.api.server.generated.models.DomainVerificationResponse
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.OrganizationDomainVerificationService
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/v1/domain_verifications")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DomainVerificationApiController(
  private val organizationDomainVerificationService: OrganizationDomainVerificationService,
  private val currentUserService: CurrentUserService,
  private val roleResolver: RoleResolver,
) : DomainVerificationsApi {
  @RequiresIntent(Intent.ManageDomainVerification)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createDomainVerification(domainVerificationCreateRequestBody: DomainVerificationCreateRequestBody): DomainVerificationResponse {
    try {
      val savedDomainModel =
        organizationDomainVerificationService.createDomainVerification(
          organizationId = domainVerificationCreateRequestBody.organizationId,
          domain = domainVerificationCreateRequestBody.domain,
          createdBy = currentUserService.getCurrentUser().userId,
        )
      return savedDomainModel.toApiResponse()
    } catch (e: IllegalArgumentException) {
      throw BadRequestProblem(
        detail = e.message,
        data = ProblemMessageData().message(e.message),
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @RequiresIntent(Intent.ManageDomainVerification)
  override fun listDomainVerifications(organizationIdRequestBody: OrganizationIdRequestBody): DomainVerificationListResponse {
    val organizationId = organizationIdRequestBody.organizationId
    val domainModels = organizationDomainVerificationService.findByOrganizationId(organizationId)
    val apiResponses = domainModels.map { it.toApiResponse() }
    return DomainVerificationListResponse(domainVerifications = apiResponses)
  }

  /**
   * List all pending domain verifications for cron processing.
   * Requires instance ADMIN role because it spans all organizations.
   */
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPendingDomainVerifications(): DomainVerificationListResponse {
    val pendingVerifications = organizationDomainVerificationService.findByStatus(DomainVerificationStatus.PENDING)
    return DomainVerificationListResponse(
      domainVerifications = pendingVerifications.map { it.toApiResponse() },
    )
  }

  /**
   * Check a domain verification by performing DNS lookup.
   * Primarily called by the domain verification cronjob, but authorized to
   * organization admin users who want to initiate a manual check.
   */
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkDomainVerification(domainVerificationIdRequestBody: DomainVerificationIdRequestBody): DomainVerificationResponse {
    val verification =
      try {
        organizationDomainVerificationService.getDomainVerification(domainVerificationIdRequestBody.domainVerificationId)
      } catch (_: IllegalArgumentException) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("domain_verification")
            .resourceId(domainVerificationIdRequestBody.domainVerificationId.toString()),
        )
      }

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withRef(AuthenticationId.ORGANIZATION_ID, verification.organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    val updatedVerification =
      organizationDomainVerificationService.checkAndUpdateVerification(
        domainVerificationIdRequestBody.domainVerificationId,
      )

    return updatedVerification.toApiResponse()
  }

  /**
   * Delete a domain verification and cascade delete to organization_email_domain.
   * Removes SSO enforcement for the domain if it exists.
   */
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteDomainVerification(domainVerificationIdRequestBody: DomainVerificationIdRequestBody) {
    val verification =
      try {
        organizationDomainVerificationService.getDomainVerification(domainVerificationIdRequestBody.domainVerificationId)
      } catch (_: IllegalArgumentException) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("domain_verification")
            .resourceId(domainVerificationIdRequestBody.domainVerificationId.toString()),
        )
      }

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withRef(AuthenticationId.ORGANIZATION_ID, verification.organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    try {
      organizationDomainVerificationService.deleteDomainVerification(
        domainVerificationId = domainVerificationIdRequestBody.domainVerificationId,
      )
    } catch (e: IllegalArgumentException) {
      throw BadRequestProblem(
        detail = e.message,
        data = ProblemMessageData().message(e.message),
      )
    }
  }

  /**
   * Reset a failed or expired domain verification back to pending.
   * Allows users to retry verification after fixing DNS configuration.
   */
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun resetDomainVerification(domainVerificationIdRequestBody: DomainVerificationIdRequestBody): DomainVerificationResponse {
    val verification =
      try {
        organizationDomainVerificationService.getDomainVerification(domainVerificationIdRequestBody.domainVerificationId)
      } catch (_: IllegalArgumentException) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("domain_verification")
            .resourceId(domainVerificationIdRequestBody.domainVerificationId.toString()),
        )
      }

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withRef(AuthenticationId.ORGANIZATION_ID, verification.organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    try {
      val resetVerification =
        organizationDomainVerificationService.resetDomainVerification(
          domainVerificationId = domainVerificationIdRequestBody.domainVerificationId,
        )
      return resetVerification.toApiResponse()
    } catch (e: IllegalArgumentException) {
      throw BadRequestProblem(
        detail = e.message,
        data = ProblemMessageData().message(e.message),
      )
    }
  }

  private fun OrganizationDomainVerification.toApiResponse(): DomainVerificationResponse =
    DomainVerificationResponse(
      id = this.id!!,
      organizationId = this.organizationId,
      domain = this.domain,
      verificationMethod =
        when (this.verificationMethod) {
          DomainVerificationMethod.DNS_TXT -> DomainVerificationResponse.VerificationMethod.DNS_TXT
          DomainVerificationMethod.LEGACY -> DomainVerificationResponse.VerificationMethod.LEGACY
        },
      status =
        when (this.status) {
          DomainVerificationStatus.PENDING -> DomainVerificationResponse.Status.PENDING
          DomainVerificationStatus.VERIFIED -> DomainVerificationResponse.Status.VERIFIED
          DomainVerificationStatus.FAILED -> DomainVerificationResponse.Status.FAILED
          DomainVerificationStatus.EXPIRED -> DomainVerificationResponse.Status.EXPIRED
        },
      dnsRecordName = this.dnsRecordName,
      dnsRecordValue = this.verificationToken?.let { "${OrganizationDomainVerificationService.DNS_RECORD_VALUE_PREFIX}$it" },
      attempts = this.attempts,
      expiresAt = this.expiresAt?.toEpochSecond(),
      verifiedAt = this.verifiedAt?.toEpochSecond(),
      lastCheckedAt = this.lastCheckedAt?.toEpochSecond(),
      createdAt = this.createdAt!!.toEpochSecond(),
    )
}
