/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Fetch the configuration to send a notification using customerIo.
 */
@Singleton
@Requires(property = "airbyte.notification.customerio.apikey",
          notEquals = "")
@Slf4j
public class CustomerIoEmailConfigFetcherImpl implements CustomerIoEmailConfigFetcher {

  private final WorkspaceApi workspaceApi;

  public CustomerIoEmailConfigFetcherImpl(WorkspaceApi workspaceApi) {
    this.workspaceApi = workspaceApi;
  }

  @Nullable
  @Override
  public CustomerIoEmailConfig fetchConfig(@NotNull final UUID connectionId) {
    ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(connectionId);

    try {
      return new CustomerIoEmailConfig(workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody).getEmail());
    } catch (ApiException e) {
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
