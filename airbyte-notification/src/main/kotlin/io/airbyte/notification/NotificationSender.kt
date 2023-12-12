package io.airbyte.notification

import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.io.IOException

interface NotificationSender<T> {
  fun sendNotification(
    config: T,
    subject: String,
    message: String,
  )

  fun notificationType(): NotificationType
}

@Singleton
class WebhookNotificationSender(
  @Named("webhookHttpClient") private val okHttpClient: OkHttpClient,
) : NotificationSender<WebhookConfig> {
  companion object {
    private val log = LoggerFactory.getLogger(WebhookNotificationSender::class.java)
  }

  override fun sendNotification(
    config: WebhookConfig,
    subject: String,
    message: String,
  ) {
    val requestBody: RequestBody = """{"text": "$message"}""".toRequestBody("application/json".toMediaType())

    val request: okhttp3.Request =
      okhttp3.Request.Builder()
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

  override fun notificationType(): NotificationType {
    return NotificationType.WEBHOOK
  }
}
