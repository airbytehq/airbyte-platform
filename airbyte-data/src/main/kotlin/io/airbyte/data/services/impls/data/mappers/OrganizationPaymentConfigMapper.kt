package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.OrganizationPaymentConfig as ModelOrganizationPaymentConfig
import io.airbyte.config.OrganizationPaymentConfig.PaymentStatus as ModelPaymentStatus
import io.airbyte.config.OrganizationPaymentConfig.UsageCategoryOverride as ModelUsageCategoryOverride
import io.airbyte.data.repositories.entities.OrganizationPaymentConfig as EntityOrganizationPaymentConfig
import io.airbyte.db.instance.configs.jooq.generated.enums.PaymentStatus as EntityPaymentStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.UsageCategoryOverride as EntityUsageCategoryOverride

fun EntityOrganizationPaymentConfig.toConfigModel(): ModelOrganizationPaymentConfig =
  ModelOrganizationPaymentConfig()
    .withOrganizationId(this.organizationId)
    .withPaymentProviderId(this.paymentProviderId)
    .withPaymentStatus(this.paymentStatus.toConfigModel())
    .withGracePeriodEndAt(this.gracePeriodEndAt?.toEpochSecond())
    .withUsageCategoryOverride(this.usageCategoryOverride?.toConfigModel())
    .withCreatedAt(this.createdAt?.toEpochSecond())
    .withUpdatedAt(this.updatedAt?.toEpochSecond())

fun ModelOrganizationPaymentConfig.toEntity(): EntityOrganizationPaymentConfig =
  EntityOrganizationPaymentConfig(
    organizationId = this.organizationId,
    paymentProviderId = this.paymentProviderId,
    paymentStatus = this.paymentStatus.toEntity(),
    gracePeriodEndAt =
      this.gracePeriodEndAt?.let {
        java.time.OffsetDateTime.ofInstant(
          java.time.Instant.ofEpochSecond(it),
          java.time.ZoneOffset.UTC,
        )
      },
    usageCategoryOverride = this.usageCategoryOverride?.toEntity(),
    createdAt = this.createdAt?.let { java.time.OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(it), java.time.ZoneOffset.UTC) },
    updatedAt = this.updatedAt?.let { java.time.OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(it), java.time.ZoneOffset.UTC) },
  )

fun EntityPaymentStatus.toConfigModel(): ModelPaymentStatus =
  when (this) {
    EntityPaymentStatus.uninitialized -> ModelPaymentStatus.UNINITIALIZED
    EntityPaymentStatus.okay -> ModelPaymentStatus.OKAY
    EntityPaymentStatus.grace_period -> ModelPaymentStatus.GRACE_PERIOD
    EntityPaymentStatus.disabled -> ModelPaymentStatus.DISABLED
    EntityPaymentStatus.locked -> ModelPaymentStatus.LOCKED
    EntityPaymentStatus.manual -> ModelPaymentStatus.MANUAL
  }

fun ModelPaymentStatus.toEntity(): EntityPaymentStatus =
  when (this) {
    ModelPaymentStatus.UNINITIALIZED -> EntityPaymentStatus.uninitialized
    ModelPaymentStatus.OKAY -> EntityPaymentStatus.okay
    ModelPaymentStatus.GRACE_PERIOD -> EntityPaymentStatus.grace_period
    ModelPaymentStatus.DISABLED -> EntityPaymentStatus.disabled
    ModelPaymentStatus.LOCKED -> EntityPaymentStatus.locked
    ModelPaymentStatus.MANUAL -> EntityPaymentStatus.manual
  }

fun EntityUsageCategoryOverride.toConfigModel(): ModelUsageCategoryOverride =
  when (this) {
    EntityUsageCategoryOverride.free -> ModelUsageCategoryOverride.FREE
    EntityUsageCategoryOverride.internal -> ModelUsageCategoryOverride.INTERNAL
  }

fun ModelUsageCategoryOverride.toEntity(): EntityUsageCategoryOverride =
  when (this) {
    ModelUsageCategoryOverride.FREE -> EntityUsageCategoryOverride.free
    ModelUsageCategoryOverride.INTERNAL -> EntityUsageCategoryOverride.internal
  }
