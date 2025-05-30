/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import com.google.common.base.Preconditions;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.protocol.models.v0.ConnectorSpecification;

/**
 * Convert between API and internal versions of spec models.
 */
public class SpecFetcher {

  public static ConnectorSpecification getSpecFromJob(final SynchronousResponse<ConnectorSpecification> response) {
    Preconditions.checkState(response.isSuccess(), "Get Spec job failed.");
    Preconditions.checkNotNull(response.getOutput(), "Get Spec job return null spec");

    return response.getOutput();
  }

}
