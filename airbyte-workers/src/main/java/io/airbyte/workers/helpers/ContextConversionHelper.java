/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectionContext;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import jakarta.annotation.Nullable;

public class ContextConversionHelper {

  public static ActorContext connectionContextToSourceContext(ConnectionContext connectionContext) {
    if (connectionContext == null) {
      return new ActorContext();
    }
    return new ActorContext()
        .withActorId(connectionContext.getSourceId())
        .withActorType(ActorType.SOURCE)
        .withActorDefinitionId(connectionContext.getSourceDefinitionId())
        .withWorkspaceId(connectionContext.getWorkspaceId())
        .withOrganizationId(connectionContext.getOrganizationId());
  }

  public static ActorContext connectionContextToDestinationContext(ConnectionContext connectionContext) {
    if (connectionContext == null) {
      return new ActorContext();
    }
    return new ActorContext()
        .withActorId(connectionContext.getDestinationId())
        .withActorType(ActorType.DESTINATION)
        .withActorDefinitionId(connectionContext.getDestinationDefinitionId())
        .withWorkspaceId(connectionContext.getWorkspaceId())
        .withOrganizationId(connectionContext.getOrganizationId());
  }

  public static ActorContext buildSourceContextFrom(final @Nullable JobInput jobInputs, final SyncJobCheckConnectionInputs checkInputs) {
    if (jobInputs != null && jobInputs.getSyncInput() != null) {
      return ContextConversionHelper.connectionContextToSourceContext(jobInputs.getSyncInput().getConnectionContext());
    } else {
      return checkInputs.getSourceCheckConnectionInput().getActorContext();
    }
  }

  public static ActorContext buildDestinationContextFrom(final @Nullable JobInput jobInputs, final SyncJobCheckConnectionInputs checkInputs) {
    if (jobInputs != null && jobInputs.getSyncInput() != null) {
      return ContextConversionHelper.connectionContextToDestinationContext(jobInputs.getSyncInput().getConnectionContext());
    } else {
      return checkInputs.getDestinationCheckConnectionInput().getActorContext();
    }
  }

}
