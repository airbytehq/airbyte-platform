/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.PaymentStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.SubscriptionStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.UsageCategoryOverride
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("organization_payment_config")
open class OrganizationPaymentConfig(
  @field:Id
  var organizationId: UUID,
  var paymentProviderId: String? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var paymentStatus: PaymentStatus = PaymentStatus.uninitialized,
  @field:TypeDef(type = DataType.OBJECT)
  var subscriptionStatus: SubscriptionStatus = SubscriptionStatus.pre_subscription,
  var gracePeriodEndAt: java.time.OffsetDateTime? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var usageCategoryOverride: UsageCategoryOverride? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)
