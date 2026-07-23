/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.config.ConnectionContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.api.client.model.generated.ConnectionContextRead as ConnectionContextReadClientApiModel
import io.airbyte.api.model.generated.ConnectionContextRead as ConnectionContextReadServerApiModel

class CommonConvertersTest {
  @Test
  fun `ConnectionContextReadClientApiModel#toInternal converts from api client model to config model`() {
    val apiClientModel =
      ConnectionContextReadClientApiModel(
        connectionId = UUID.randomUUID(),
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
      )

    val result = apiClientModel.toInternal()
    val expected = ConnectionContext()
    expected.connectionId = apiClientModel.connectionId
    expected.sourceId = apiClientModel.sourceId
    expected.destinationId = apiClientModel.destinationId
    expected.sourceDefinitionId = apiClientModel.sourceDefinitionId
    expected.destinationDefinitionId = apiClientModel.destinationDefinitionId
    expected.workspaceId = apiClientModel.workspaceId
    expected.organizationId = apiClientModel.organizationId

    assertEquals(expected, result)
  }

  @Test
  fun `ConnectionContext#toServerApi converts from config model to server api model`() {
    val configModel = ConnectionContext()
    configModel.connectionId = UUID.randomUUID()
    configModel.sourceId = UUID.randomUUID()
    configModel.destinationId = UUID.randomUUID()
    configModel.sourceDefinitionId = UUID.randomUUID()
    configModel.destinationDefinitionId = UUID.randomUUID()
    configModel.workspaceId = UUID.randomUUID()
    configModel.organizationId = UUID.randomUUID()

    val result = configModel.toServerApi()
    val expected = ConnectionContextReadServerApiModel()
    expected.connectionId = configModel.connectionId
    expected.sourceId = configModel.sourceId
    expected.destinationId = configModel.destinationId
    expected.sourceDefinitionId = configModel.sourceDefinitionId
    expected.destinationDefinitionId = configModel.destinationDefinitionId
    expected.workspaceId = configModel.workspaceId
    expected.organizationId = configModel.organizationId

    assertEquals(expected, result)
  }
}
