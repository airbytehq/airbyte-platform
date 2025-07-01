/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

private val log = KotlinLogging.logger { }

/**
 * Fetch the configuration to send a notification using customerIo.
 */
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey", notEquals = "")
class CustomerIoEmailConfigFetcherImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : CustomerIoEmailConfigFetcher {
  override fun fetchConfig(connectionId: UUID): CustomerIoEmailConfig {
    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId)

    try {
      return CustomerIoEmailConfig(airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody).email!!)
    } catch (e: IOException) {
      log.error { "Unable to fetch workspace by connection" }
      throw RuntimeException(e)
    }
  }

  override fun notificationType(): NotificationType = NotificationType.CUSTOMERIO
}
