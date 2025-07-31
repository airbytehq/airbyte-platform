/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectionContext
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.SyncJobCheckConnectionInputs
import jakarta.annotation.Nullable

object ContextConversionHelper {
  @JvmStatic
  fun connectionContextToSourceContext(connectionContext: ConnectionContext?): ActorContext {
    if (connectionContext == null) {
      return ActorContext()
    }
    return ActorContext()
      .withActorId(connectionContext.sourceId)
      .withActorType(ActorType.SOURCE)
      .withActorDefinitionId(connectionContext.sourceDefinitionId)
      .withWorkspaceId(connectionContext.workspaceId)
      .withOrganizationId(connectionContext.organizationId)
  }

  @JvmStatic
  fun connectionContextToDestinationContext(connectionContext: ConnectionContext?): ActorContext {
    if (connectionContext == null) {
      return ActorContext()
    }
    return ActorContext()
      .withActorId(connectionContext.destinationId)
      .withActorType(ActorType.DESTINATION)
      .withActorDefinitionId(connectionContext.destinationDefinitionId)
      .withWorkspaceId(connectionContext.workspaceId)
      .withOrganizationId(connectionContext.organizationId)
  }

  @JvmStatic
  fun buildSourceContextFrom(
    @Nullable jobInputs: JobInput?,
    checkInputs: SyncJobCheckConnectionInputs,
  ): ActorContext =
    if (jobInputs?.syncInput != null) {
      connectionContextToSourceContext(jobInputs.syncInput!!.connectionContext)
    } else {
      checkInputs.sourceCheckConnectionInput!!.actorContext
    }

  @JvmStatic
  fun buildDestinationContextFrom(
    @Nullable jobInputs: JobInput?,
    checkInputs: SyncJobCheckConnectionInputs,
  ): ActorContext =
    if (jobInputs?.syncInput != null) {
      connectionContextToDestinationContext(jobInputs.syncInput!!.connectionContext)
    } else {
      checkInputs.destinationCheckConnectionInput!!.actorContext
    }
}
