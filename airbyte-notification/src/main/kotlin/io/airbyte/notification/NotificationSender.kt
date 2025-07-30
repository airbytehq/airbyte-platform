/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.util.UUID

private val log = KotlinLogging.logger {}

interface NotificationSender<T> {
  fun sendNotification(
    config: T,
    subject: String,
    message: String,
    workspaceId: UUID?,
  )

  fun notificationType(): NotificationType
}

@Singleton
class WebhookNotificationSender(
  @Named("webhookHttpClient") private val okHttpClient: OkHttpClient,
) : NotificationSender<WebhookConfig> {
  companion object {
  }

  override fun sendNotification(
    config: WebhookConfig,
    subject: String,
    message: String,
    workspaceId: UUID?,
  ) {
    val requestBody: RequestBody = """{"text": "$message"}""".toRequestBody("application/json".toMediaType())

    val request: okhttp3.Request =
      okhttp3.Request
        .Builder()
        .url(config.webhookUrl)
        .post(requestBody)
        .build()

    okHttpClient.newCall(request).execute().use { response ->
      if (response.isSuccessful) {
        log.info("Successful notification (${response.code}): {${response.body}")
      } else {
        throw IOException("Failed to  notification (${response.code}): ${response.body}")
      }
    }
  }

  override fun notificationType(): NotificationType = NotificationType.WEBHOOK
}
