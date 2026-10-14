/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("billing_notification_settings")
open class BillingNotificationSettings(
  @field:Id
  var organizationId: UUID,
  var recipientEmail: String? = null,
  var syncCreditThreshold: BigDecimal = BigDecimal.ZERO,
  var invoiceAmountThreshold: BigDecimal = BigDecimal.ZERO,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
