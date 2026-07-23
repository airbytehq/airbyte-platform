/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.config.ConnectionContext
import io.airbyte.api.client.model.generated.ConnectionContextRead as ConnectionContextReadClientApiModel
import io.airbyte.api.model.generated.ConnectionContextRead as ConnectionContextReadServerApiModel

/*
 * Api <-> Domain model converters for common shared objects.
 */

fun ConnectionContextReadClientApiModel.toInternal(): ConnectionContext =
  ConnectionContext()
    .withConnectionId(this.connectionId)
    .withSourceId(this.sourceId)
    .withDestinationId(this.destinationId)
    .withSourceDefinitionId(this.sourceDefinitionId)
    .withDestinationDefinitionId(this.destinationDefinitionId)
    .withWorkspaceId(this.workspaceId)
    .withOrganizationId(this.organizationId)

fun ConnectionContext.toServerApi(): ConnectionContextReadServerApiModel {
  val apiModel = ConnectionContextReadServerApiModel()
  apiModel.connectionId = this.connectionId
  apiModel.sourceId = this.sourceId
  apiModel.destinationId = this.destinationId
  apiModel.sourceDefinitionId = this.sourceDefinitionId
  apiModel.destinationDefinitionId = this.destinationDefinitionId
  apiModel.workspaceId = this.workspaceId
  apiModel.organizationId = this.organizationId
  return apiModel
}
