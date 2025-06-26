/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.commons.resources.Resources
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.MediaType
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.apache.http.HttpHeaders
import java.io.IOException

private val log = KotlinLogging.logger { }

/**
 * Send a notification using customerIo.
 */
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey", notEquals = "")
@Replaces(
  FakeCustomerIoEmailNotificationSender::class,
)
open class CustomerIoEmailNotificationSender(
  @param:Named("webhookHttpClient") private val okHttpClient: OkHttpClient,
  @param:Value("\${airbyte.notification.customerio.apikey}") private val apiToken: String,
) : NotificationSender<CustomerIoEmailConfig> {
  override fun sendNotification(
    config: CustomerIoEmailConfig,
    subject: String,
    message: String,
  ) {
    val formattedMessage = message.replace("\n", "<br>")
    val custumerIoPayload =
      renderTemplate(
        CUSTOMER_IO_DEFAULT_TEMPLATE,
        BREAKING_CHANGE_TRANSACTION_MESSAGE_ID, // to_email=
        config.to, // identifier_email=
        config.to, // subject=
        subject, // email_title=
        subject, // email_body=
        formattedMessage,
      )
    callCustomerIoSendNotification(custumerIoPayload)
  }

  /**
   * Invitation emails will be sent on a separate CustomerIO email templates. This does not apply to
   * slack notifications thus it is only implemented here.
   */
  fun sendNotificationOnInvitingNewUser(
    config: CustomerIoEmailConfig,
    inviterUserName: String,
    workspaceName: String,
    inviteUrl: String,
  ) {
    val custumerIoPayload =
      renderTemplate(
        INVITATION_NEW_USER_TEMPLATE,
        INVITATION_NEW_USER_TRANSACTION_MESSAGE_ID, // to_email=
        config.to, // identifier_email=
        config.to, // name=
        inviterUserName, // workspace_name=
        workspaceName, // invite_url=
        inviteUrl,
      )
    callCustomerIoSendNotification(custumerIoPayload)
  }

  fun sendInviteToUser(
    config: CustomerIoEmailConfig,
    inviterUserName: String?,
    inviteUrl: String?,
  ) {
    val costumerIoPayload =
      renderTemplate(
        INVITE_USER_TEMPLATE,
        INVITE_USER_TRANSACTION_MESSAGE_ID, // to_email=
        config.to, // identifier_email=
        config.to, // name=
        inviterUserName!!, // invite_url=
        inviteUrl!!,
      )
    callCustomerIoSendNotification(costumerIoPayload)
  }

  /**
   * Invitation emails will be sent on a separate CustomerIO email templates. This does not apply to
   * slack notifications thus it is only implemented here.
   */
  fun sendNotificationOnInvitingExistingUser(
    config: CustomerIoEmailConfig,
    inviterUserName: String?,
    workspaceName: String?,
  ) {
    val custumerIoPayload =
      renderTemplate(
        INVITATION_EXISTING_USER_TEMPLATE,
        INVITATION_EXISTING_USER_TRANSACTION_MESSAGE_ID, // to_email=
        config.to, // identifier_email=
        config.to, // name=
        inviterUserName!!, // workspace_name=
        workspaceName!!,
      )
    callCustomerIoSendNotification(custumerIoPayload)
  }

  /**
   * Invitation emails will be sent on a separate CustomerIO email templates. This does not apply to
   * slack notifications thus it is only implemented here.
   */
  fun resendNotificationOnInvitingNewUser(
    config: CustomerIoEmailConfig,
    workspaceName: String?,
    inviteUrl: String?,
  ) {
    val custumerIoPayload =
      renderTemplate(
        INVITATION_RESEND_TEMPLATE,
        INVITATION_RESEND_TRANSACTION_MESSAGE_ID,
        // to_email=
        config.to,
        // identifier_email=
        config.to,
        // workspace_name=
        workspaceName!!,
        // invite_url=
        inviteUrl!!,
      )
    callCustomerIoSendNotification(custumerIoPayload)
  }

  protected open fun callCustomerIoSendNotification(custumerIoPayload: String) {
    val requestBody: RequestBody = custumerIoPayload.toRequestBody(JSON)

    val request =
      Request
        .Builder()
        .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(HttpHeaders.AUTHORIZATION, "Bearer $apiToken")
        .url(CUSTOMER_IO_URL)
        .post(requestBody)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        if (response.isSuccessful) {
          log.info("Successful notification ({}): {}", response.code, response.body!!.string())
        } else {
          throw IOException(String.format("Failed to  notification: code(%s), body(%s)", response.code, response.body!!.string()))
        }
      }
    } catch (e: Exception) {
      log.error(e) { "Fail to notify" }
      throw RuntimeException(e)
    }
  }

  override fun notificationType(): NotificationType = NotificationType.CUSTOMERIO

  private fun renderTemplate(
    templateFile: String,
    vararg data: String,
  ): String {
    val template: String
    try {
      template = Resources.read(templateFile)
    } catch (e: IOException) {
      log.error { "Unable to render the customer io notification." }
      throw RuntimeException(e)
    }
    return String.format(template, *data)
  }

  companion object {
    val JSON: MediaType = "application/json; charset=utf-8".toMediaType()

    private const val CUSTOMER_IO_URL = "https://api.customer.io/v1/send/email"
    private const val CUSTOMER_IO_DEFAULT_TEMPLATE = "customerio/default_template.json"
    private const val INVITATION_NEW_USER_TEMPLATE = "customerio/invitation_new_user_template.json"
    private const val INVITE_USER_TEMPLATE = "customerio/invite_user_template.json"
    private const val INVITATION_RESEND_TEMPLATE = "customerio/invitation_resend_template.json"
    private const val INVITATION_EXISTING_USER_TEMPLATE = "customerio/invitation_existing_user_template.json"
    private const val BREAKING_CHANGE_TRANSACTION_MESSAGE_ID = "6"
    private const val INVITATION_NEW_USER_TRANSACTION_MESSAGE_ID = "11"
    private const val INVITATION_EXISTING_USER_TRANSACTION_MESSAGE_ID = "12"
    private const val INVITATION_RESEND_TRANSACTION_MESSAGE_ID = "17"
    private const val INVITE_USER_TRANSACTION_MESSAGE_ID = "28"
  }
}
