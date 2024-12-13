package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OrganizationPaymentConfigApi
import io.airbyte.api.model.generated.OrganizationPaymentConfigRead
import io.airbyte.api.model.generated.OrganizationPaymentConfigUpdateRequestBody
import io.airbyte.api.problems.ResourceType
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.OrganizationId
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.OrganizationService
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.OrganizationPaymentConfig.PaymentStatus
import io.airbyte.config.OrganizationPaymentConfig.UsageCategoryOverride
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.UUID
import io.airbyte.data.services.OrganizationPaymentConfigService as OrganizationPaymentConfigRepository

private val UTC = ZoneId.of("UTC")

@Controller("/api/v1/organization_payment_config")
open class OrganizationPaymentConfigController(
  private val organizationService: OrganizationService,
  private val organizationPaymentConfigRepository: OrganizationPaymentConfigRepository,
) : OrganizationPaymentConfigApi {
  @RequiresIntent(Intent.ManageOrganizationPaymentConfigs)
  @Get("/{organizationId}")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganizationPaymentConfig(
    @PathVariable("organizationId") organizationId: UUID,
  ): OrganizationPaymentConfigRead =
    organizationPaymentConfigRepository.findByOrganizationId(organizationId)?.toApiModel()
      ?: throw ResourceNotFoundProblem(
        ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
      )

  @RequiresIntent(Intent.ManageOrganizationPaymentConfigs)
  @Post("/{organizationId}/end_grace_period")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun endGracePeriod(
    @PathVariable("organizationId") organizationId: UUID,
  ) {
    organizationService.handlePaymentGracePeriodEnded(OrganizationId(organizationId))
  }

  @RequiresIntent(Intent.ManageOrganizationPaymentConfigs)
  @Post
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateOrganizationPaymentConfig(
    @Body requestBody: OrganizationPaymentConfigUpdateRequestBody,
  ): OrganizationPaymentConfigRead {
    val existingConfig =
      organizationPaymentConfigRepository.findByOrganizationId(requestBody.organizationId) ?: throw ResourceNotFoundProblem(
        ProblemResourceData().resourceId(requestBody.organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
      )

    existingConfig.apply {
      paymentProviderId = requestBody.paymentProviderId
      paymentStatus = PaymentStatus.fromValue(requestBody.paymentStatus.value())
      usageCategoryOverride = requestBody.usageCategoryOverwrite?.let { UsageCategoryOverride.fromValue(it.value()) }
    }

    organizationPaymentConfigRepository.savePaymentConfig(existingConfig)
    return getOrganizationPaymentConfig(requestBody.organizationId)
  }
}

private fun OrganizationPaymentConfig.toApiModel(): OrganizationPaymentConfigRead =
  OrganizationPaymentConfigRead()
    .organizationId(this.organizationId)
    .paymentStatus(OrganizationPaymentConfigRead.PaymentStatusEnum.fromValue(this.paymentStatus.value()))
    .subscriptionStatus(OrganizationPaymentConfigRead.SubscriptionStatusEnum.fromValue(this.subscriptionStatus.value()))
    .paymentProviderId(this.paymentProviderId)
    .gracePeriodEndAt(this.gracePeriodEndAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), UTC) })
    .usageCategoryOverwrite(this.usageCategoryOverride?.let { OrganizationPaymentConfigRead.UsageCategoryOverwriteEnum.fromValue(it.value()) })
