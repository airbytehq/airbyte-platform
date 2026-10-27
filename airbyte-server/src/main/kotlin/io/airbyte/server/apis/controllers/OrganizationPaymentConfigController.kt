/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.ResourceType
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.apis.OrganizationPaymentConfigApi
import io.airbyte.api.server.generated.models.OrganizationPaymentConfigRead
import io.airbyte.api.server.generated.models.OrganizationPaymentConfigUpdateRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.OrganizationService
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.OrganizationPaymentConfig.PaymentStatus
import io.airbyte.config.OrganizationPaymentConfig.UsageCategoryOverride
import io.airbyte.domain.models.OrganizationId
import io.airbyte.server.apis.requireBody
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.PathVariable
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.UUID
import io.airbyte.data.services.OrganizationPaymentConfigService as OrganizationPaymentConfigRepository

private val UTC = ZoneId.of("UTC")

@Controller
open class OrganizationPaymentConfigController(
  protected val organizationService: OrganizationService,
  protected val organizationPaymentConfigRepository: OrganizationPaymentConfigRepository,
) : OrganizationPaymentConfigApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.OrganizationPaymentConfigRule::class,
  )
  override fun getOrganizationPaymentConfig(
    @PathVariable("organizationId") organizationId: UUID,
  ): OrganizationPaymentConfigRead = readOrganizationPaymentConfig(organizationId)

  protected fun readOrganizationPaymentConfig(organizationId: UUID): OrganizationPaymentConfigRead =
    organizationPaymentConfigRepository.findByOrganizationId(organizationId)?.toApiModel()
      ?: throw ResourceNotFoundProblem(
        ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
      )

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.OrganizationPaymentConfigRule::class,
  )
  override fun endGracePeriod(
    @PathVariable("organizationId") organizationId: UUID,
  ) {
    organizationService.handlePaymentGracePeriodEnded(OrganizationId(organizationId))
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.OrganizationPaymentConfigRule::class,
  )
  override fun updateOrganizationPaymentConfig(
    @Body requestBody: OrganizationPaymentConfigUpdateRequestBody?,
  ): OrganizationPaymentConfigRead {
    val body = requestBody.requireBody()
    val existingConfig =
      organizationPaymentConfigRepository.findByOrganizationId(body.organizationId) ?: throw ResourceNotFoundProblem(
        ProblemResourceData().resourceId(body.organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
      )

    existingConfig.apply {
      paymentProviderId = body.paymentProviderId
      paymentStatus = PaymentStatus.fromValue(body.paymentStatus.value)
      usageCategoryOverride = body.usageCategoryOverwrite?.let { UsageCategoryOverride.fromValue(it.value) }
    }

    organizationPaymentConfigRepository.savePaymentConfig(existingConfig)
    return readOrganizationPaymentConfig(body.organizationId)
  }
}

private fun OrganizationPaymentConfig.toApiModel(): OrganizationPaymentConfigRead =
  OrganizationPaymentConfigRead(
    organizationId = this.organizationId,
    paymentStatus = OrganizationPaymentConfigRead.PaymentStatus.entries.first { it.value == this.paymentStatus.value() },
    subscriptionStatus = OrganizationPaymentConfigRead.SubscriptionStatus.entries.first { it.value == this.subscriptionStatus.value() },
    paymentProviderId = this.paymentProviderId,
    gracePeriodEndAt = this.gracePeriodEndAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), UTC) },
    usageCategoryOverwrite =
      this.usageCategoryOverride?.let { override ->
        OrganizationPaymentConfigRead.UsageCategoryOverwrite.entries.first {
          it.value ==
            override.value()
        }
      },
  )
