/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.commons.resources.MoreResources;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.HttpHeaders;
import org.jetbrains.annotations.NotNull;

/**
 * Send a notification using customerIo.
 */
@Slf4j
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey",
          notEquals = "")
public class CustomerIoEmailNotificationSender implements NotificationSender<CustomerIoEmailConfig> {

  public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static final String CUSTOMER_IO_URL = "https://api.customer.io/v1/send/email";
  private static final String CUSTOMER_IO_DEFAULT_TEMPLATE = "customerio/default_template.json";
  private static final String BREAKING_CHANGE_TRANSACTION_MESSAGE_ID = "6";

  private final OkHttpClient okHttpClient;
  private final String apiToken;

  public CustomerIoEmailNotificationSender(@Named("webhookHttpClient") final OkHttpClient okHttpClient,
                                           @Value("${airbyte.notification.customerio.apikey}") final String apiToken) {
    this.okHttpClient = okHttpClient;
    this.apiToken = apiToken;
  }

  @Override
  public void sendNotification(final CustomerIoEmailConfig config, @NotNull final String subject, @NotNull final String message) {
    final String custumerIoPayload;
    final String formattedMessage = message.replace("\n", "<br>");
    try {
      custumerIoPayload = renderTemplate(CUSTOMER_IO_DEFAULT_TEMPLATE, BREAKING_CHANGE_TRANSACTION_MESSAGE_ID,
          config.getTo(), config.getTo(), subject, subject, formattedMessage);
    } catch (final IOException e) {
      log.error("Unable to render the customer io notification.");
      throw new RuntimeException(e);
    }

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
    return NotificationType.customerio;
  }

  private String renderTemplate(final String templateFile, final String... data) throws IOException {
    final String template = MoreResources.readResource(templateFile);
    return String.format(template, data);
  }

}
