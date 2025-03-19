/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.commons.resources.MoreResources;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.HttpHeaders;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send a notification using customerIo.
 */
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey",
          notEquals = "")
@Replaces(FakeCustomerIoEmailNotificationSender.class)
@SuppressWarnings({"PMD.ExceptionAsFlowControl", "PMD.ConfusingArgumentToVarargsMethod"})
public class CustomerIoEmailNotificationSender implements NotificationSender<CustomerIoEmailConfig> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static final String CUSTOMER_IO_URL = "https://api.customer.io/v1/send/email";
  private static final String CUSTOMER_IO_DEFAULT_TEMPLATE = "customerio/default_template.json";
  private static final String INVITATION_NEW_USER_TEMPLATE = "customerio/invitation_new_user_template.json";
  private static final String INVITE_USER_TEMPLATE = "customerio/invite_user_template.json";
  private static final String INVITATION_RESEND_TEMPLATE = "customerio/invitation_resend_template.json";
  private static final String INVITATION_EXISTING_USER_TEMPLATE = "customerio/invitation_existing_user_template.json";
  private static final String BREAKING_CHANGE_TRANSACTION_MESSAGE_ID = "6";
  private static final String INVITATION_NEW_USER_TRANSACTION_MESSAGE_ID = "11";
  private static final String INVITATION_EXISTING_USER_TRANSACTION_MESSAGE_ID = "12";
  private static final String INVITATION_RESEND_TRANSACTION_MESSAGE_ID = "17";
  private static final String INVITE_USER_TRANSACTION_MESSAGE_ID = "28";

  private final OkHttpClient okHttpClient;
  private final String apiToken;

  public CustomerIoEmailNotificationSender(@Named("webhookHttpClient") final OkHttpClient okHttpClient,
                                           @Value("${airbyte.notification.customerio.apikey}") final String apiToken) {
    this.okHttpClient = okHttpClient;
    this.apiToken = apiToken;
  }

  @Override
  public void sendNotification(final CustomerIoEmailConfig config, @NotNull final String subject, @NotNull final String message) {
    final String formattedMessage = message.replace("\n", "<br>");
    final String custumerIoPayload = renderTemplate(
        CUSTOMER_IO_DEFAULT_TEMPLATE,
        BREAKING_CHANGE_TRANSACTION_MESSAGE_ID,
        /* to_email= */ config.getTo(),
        /* identifier_email= */ config.getTo(),
        /* subject= */ subject,
        /* email_title= */ subject,
        /* email_body= */ formattedMessage);
    callCustomerIoSendNotification(custumerIoPayload);
  }

  /**
   * Invitation emails will be sent on a separate CustomerIO email templates. This does not apply to
   * slack notifications thus it is only implemented here.
   */
  public void sendNotificationOnInvitingNewUser(final CustomerIoEmailConfig config,
                                                final String inviterUserName,
                                                final String workspaceName,
                                                final String inviteUrl) {
    final String custumerIoPayload = renderTemplate(INVITATION_NEW_USER_TEMPLATE,
        INVITATION_NEW_USER_TRANSACTION_MESSAGE_ID,
        /* to_email= */config.getTo(),
        /* identifier_email= */ config.getTo(),
        /* name= */inviterUserName,
        /* workspace_name= */workspaceName,
        /* invite_url= */inviteUrl);
    callCustomerIoSendNotification(custumerIoPayload);
  }

  public void sendInviteToUser(final CustomerIoEmailConfig config,
                               final String inviterUserName,
                               final String inviteUrl) {
    final String costumerIoPayload = renderTemplate(INVITE_USER_TEMPLATE,
        INVITE_USER_TRANSACTION_MESSAGE_ID,
        /* to_email= */config.getTo(),
        /* identifier_email= */ config.getTo(),
        /* name= */inviterUserName,
        /* invite_url= */inviteUrl);
    callCustomerIoSendNotification(costumerIoPayload);
  }

  /**
   * Invitation emails will be sent on a separate CustomerIO email templates. This does not apply to
   * slack notifications thus it is only implemented here.
   */
  public void sendNotificationOnInvitingExistingUser(final CustomerIoEmailConfig config, final String inviterUserName, final String workspaceName) {
    final String custumerIoPayload =
        renderTemplate(INVITATION_EXISTING_USER_TEMPLATE, INVITATION_EXISTING_USER_TRANSACTION_MESSAGE_ID,
            /* to_email= */config.getTo(),
            /* identifier_email= */ config.getTo(),
            /* name= */inviterUserName,
            /* workspace_name= */workspaceName);
    callCustomerIoSendNotification(custumerIoPayload);
  }

  /**
   * Invitation emails will be sent on a separate CustomerIO email templates. This does not apply to
   * slack notifications thus it is only implemented here.
   */
  public void resendNotificationOnInvitingNewUser(final CustomerIoEmailConfig config, final String workspaceName, final String inviteUrl) {
    final String custumerIoPayload =
        renderTemplate(INVITATION_RESEND_TEMPLATE, INVITATION_RESEND_TRANSACTION_MESSAGE_ID, /* to_email= */config.getTo(),
            /* identifier_email= */ config.getTo(),
            /* workspace_name= */workspaceName,
            /* invite_url= */inviteUrl);
    callCustomerIoSendNotification(custumerIoPayload);
  }

  protected void callCustomerIoSendNotification(final String custumerIoPayload) {

    final RequestBody requestBody = RequestBody.create(custumerIoPayload, JSON);

    final okhttp3.Request request = new Request.Builder()
        .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiToken)
        .url(CUSTOMER_IO_URL)
        .post(requestBody)
        .build();

    try (final Response response = okHttpClient.newCall(request).execute()) {
      if (response.isSuccessful()) {
        log.info("Successful notification ({}): {}", response.code(), response.body().string());
      } else {
        throw new IOException(String.format("Failed to  notification: code(%s), body(%s)", response.code(), response.body().string()));
      }
    } catch (final Exception e) {
      log.error("Fail to notify", e);
      throw new RuntimeException(e);
    }
  }

  @NotNull
  @Override
  public NotificationType notificationType() {
    return NotificationType.CUSTOMERIO;
  }

  private String renderTemplate(final String templateFile, final String... data) {
    String template;
    try {
      template = MoreResources.readResource(templateFile);
    } catch (final IOException e) {
      log.error("Unable to render the customer io notification.");
      throw new RuntimeException(e);
    }
    return String.format(template, data);
  }

}
