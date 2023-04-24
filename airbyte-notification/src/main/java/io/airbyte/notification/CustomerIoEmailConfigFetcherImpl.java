/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Fetch the configuration to send a notification using customerIo.
 */
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey",
          notEquals = "")
public class CustomerIoEmailConfigFetcherImpl implements CustomerIoEmailConfigFetcher {

  private final String recipientEmail;

  public CustomerIoEmailConfigFetcherImpl(@Value("${airbyte.notification.customerio.recipientEmail}") final String recipientEmail) {
    this.recipientEmail = recipientEmail;
  }

  @Nullable
  @Override
  public CustomerIoEmailConfig fetchConfig(@NotNull final UUID connectionId) {
    return new CustomerIoEmailConfig(recipientEmail);
  }

  @NotNull
  @Override
  public NotificationType notificationType() {
    return NotificationType.customerio;
  }

}
