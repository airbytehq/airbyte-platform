/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.apis.DomainVerificationsApi
import io.airbyte.api.server.generated.models.DomainVerificationCreateRequestBody
import io.airbyte.api.server.generated.models.DomainVerificationListResponse
import io.airbyte.api.server.generated.models.DomainVerificationResponse
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.OrganizationDomainVerificationService
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn

@Controller("/v1/domain_verifications")
open class DomainVerificationApiController(
  private val domainVerificationService: OrganizationDomainVerificationService,
  private val currentUserService: CurrentUserService,
) : DomainVerificationsApi {
  @RequiresIntent(Intent.ManageDomainVerification)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createDomainVerification(domainVerificationCreateRequestBody: DomainVerificationCreateRequestBody): DomainVerificationResponse {
    try {
      val savedDomainModel =
        domainVerificationService.createDomainVerification(
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

    val domainModels = domainVerificationService.findByOrganizationId(organizationId)
    val apiResponses = domainModels.map { it.toApiResponse() }
    return DomainVerificationListResponse(domainVerifications = apiResponses)
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
      dnsRecordValue = this.verificationToken?.let { "airbyte-domain-verification=$it" },
      attempts = this.attempts,
      expiresAt = this.expiresAt?.toEpochSecond(),
      verifiedAt = this.verifiedAt?.toEpochSecond(),
      createdAt = this.createdAt!!.toEpochSecond(),
    )
}
