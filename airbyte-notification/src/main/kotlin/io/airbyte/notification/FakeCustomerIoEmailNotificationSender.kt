/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.metrics.MetricClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import java.util.UUID

private val log = KotlinLogging.logger { }

/**
 * Fake client for customerIoNotification - this will not send any actual emails.
 */
@Singleton
class FakeCustomerIoEmailNotificationSender internal constructor() : CustomerIoEmailNotificationSender(OkHttpClient(), "", MetricClient(null)) {
  override fun callCustomerIoSendNotification(
    customerIoPayload: String,
    workspaceId: UUID?,
  ) {
    log.info { "FakeCustomerIoEmailNotificationSender: callCustomerIoSendNotification: $customerIoPayload" }
  }
}
