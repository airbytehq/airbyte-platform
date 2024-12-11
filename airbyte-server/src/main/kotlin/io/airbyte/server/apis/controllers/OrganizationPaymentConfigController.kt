package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OrganizationPaymentConfigApi
import io.airbyte.api.model.generated.OrganizationPaymentConfigRead
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
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Delete
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.UUID
import io.airbyte.data.services.OrganizationPaymentConfigService as OrganizationPaymentConfigRepository
import io.airbyte.data.services.OrganizationService as OrganizationRepository

private val UTC = ZoneId.of("UTC")

@Controller("/api/v1/organization_payment_config")
open class OrganizationPaymentConfigController(
  private val organizationService: OrganizationService,
  private val organizationPaymentConfigRepository: OrganizationPaymentConfigRepository,
  private val organizationRepository: OrganizationRepository,
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
  @Delete("/{organizationId}")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  override fun deleteOrganizationPaymentConfig(
    @PathVariable("organizationId") organizationId: UUID,
  ) {
    if (organizationPaymentConfigRepository.findByOrganizationId(organizationId) == null) {
      throw ResourceNotFoundProblem(
        ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
      )
    }
    organizationPaymentConfigRepository.deletePaymentConfig(organizationId)
  }

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
    @Body organizationPaymentConfigUpdateRequestBody: OrganizationPaymentConfigRead,
  ): OrganizationPaymentConfigRead {
    val orgId = organizationPaymentConfigUpdateRequestBody.organizationId
    if (organizationRepository.getOrganization(orgId).isEmpty) {
      throw ResourceNotFoundProblem(ProblemResourceData().resourceId(orgId.toString()).resourceType(ResourceType.ORGANIZATION))
    }
    organizationPaymentConfigRepository.savePaymentConfig(organizationPaymentConfigUpdateRequestBody.toConfigModel())
    return getOrganizationPaymentConfig(orgId)
  }
}

private fun OrganizationPaymentConfig.toApiModel(): OrganizationPaymentConfigRead =
  OrganizationPaymentConfigRead()
    .organizationId(this.organizationId)
    .paymentStatus(OrganizationPaymentConfigRead.PaymentStatusEnum.fromValue(this.paymentStatus.value()))
    .paymentProviderId(this.paymentProviderId)
    .gracePeriodEndAt(this.gracePeriodEndAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), UTC) })
    .usageCategoryOverwrite(this.usageCategoryOverride?.let { OrganizationPaymentConfigRead.UsageCategoryOverwriteEnum.fromValue(it.value()) })

private fun OrganizationPaymentConfigRead.toConfigModel(): OrganizationPaymentConfig =
  OrganizationPaymentConfig().also {
    it.organizationId = this.organizationId
    it.paymentStatus = PaymentStatus.fromValue(this.paymentStatus.value())
    it.paymentProviderId = this.paymentProviderId
    it.gracePeriodEndAt = this.gracePeriodEndAt?.toEpochSecond()
    it.usageCategoryOverride = this.usageCategoryOverwrite?.let { UsageCategoryOverride.fromValue(it.value()) }
  }
