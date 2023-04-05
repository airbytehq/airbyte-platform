package io.airbyte.notification

import io.airbyte.api.client.model.generated.NotificationType as ApiNotificationType
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.util.UUID

interface ConfigFetcher<T> {
    fun fetchConfig(connectionId: UUID): T?
    fun notificationType(): NotificationType
}

data class WebhookConfig(val webhookUrl: String)

@Singleton
class WebhookConfigFetcher(private val workspaceApiClient: WorkspaceApi) : ConfigFetcher<WebhookConfig> {

    override fun fetchConfig(connectionId: UUID): WebhookConfig? {
        val workspaceRead: WorkspaceRead? = ConnectionIdRequestBody().connectionId(connectionId).let { workspaceApiClient.getWorkspaceByConnectionId(it) }

        return workspaceRead
                ?.notifications
                ?.firstOrNull { it.notificationType == ApiNotificationType.SLACK }
                ?.slackConfiguration
                ?.let { WebhookConfig(it.webhook) }
    }

    override fun notificationType(): NotificationType = NotificationType.webhook
}

@Singleton
@Requires(property = "airbyte.notification.webhook.url")
data class SendGridEmailConfig(val from: String, val to: String)

@Singleton
@Requires(property = "airbyte.notification.sengrid.apikey")
class SendGridEmailConfigFetcher(@Value("airbyte.notification.sengrid.senderEmail") private val senderEmail: String,
                                 @Value("airbyte.notification.sengrid.recipientEmail") private val recipientEmail: String,
        ): ConfigFetcher<SendGridEmailConfig> {

    override fun fetchConfig(connectionId: UUID): SendGridEmailConfig? = SendGridEmailConfig(senderEmail, recipientEmail)

    override fun notificationType(): NotificationType = NotificationType.email
}
