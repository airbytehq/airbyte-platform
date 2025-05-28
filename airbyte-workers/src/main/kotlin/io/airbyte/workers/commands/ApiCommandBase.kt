/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CancelCommandRequest
import io.airbyte.api.client.model.generated.CommandStatusRequest
import io.airbyte.api.client.model.generated.CommandStatusResponse

abstract class ApiCommandBase<Input>(
  protected val airbyteApiClient: AirbyteApiClient,
) : ConnectorCommand<Input> {
  override fun isTerminal(id: String): Boolean {
    val status = airbyteApiClient.commandApi.getCommandStatus(CommandStatusRequest(id))

    return status.status == CommandStatusResponse.Status.COMPLETED ||
      status.status == CommandStatusResponse.Status.CANCELLED
  }

  override fun cancel(id: String) {
    airbyteApiClient.commandApi.cancelCommand(
      CancelCommandRequest(id),
    )
  }
}
