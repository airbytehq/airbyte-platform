/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetch the configuration to send a notification using customerIo.
 */
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey",
          notEquals = "")
public class CustomerIoEmailConfigFetcherImpl implements CustomerIoEmailConfigFetcher {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteApiClient airbyteApiClient;

  public CustomerIoEmailConfigFetcherImpl(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  @Nullable
  @Override
  public CustomerIoEmailConfig fetchConfig(@NotNull final UUID connectionId) {
    ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody(connectionId);

    try {
      return new CustomerIoEmailConfig(airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(connectionIdRequestBody).getEmail());
    } catch (IOException e) {
      log.error("Unable to fetch workspace by connection");
      throw new RuntimeException(e);
    }
  }

  @NotNull
  @Override
  public NotificationType notificationType() {
    return NotificationType.CUSTOMERIO;
  }

}
