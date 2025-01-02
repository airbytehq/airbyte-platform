/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.temporal.exception.DeletedWorkflowException;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.metrics.lib.MetricClient;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowOptions;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ConnectionManageUtilsTest {

  @Test
  void signalAndRepairIfNeceesaryWhenNoWorkflowWillCreate() throws DeletedWorkflowException {
    final var mWorkflow = mock(WorkflowClientWrapped.class);
    final var mMetric = mock(MetricClient.class);
    final var cid = UUID.randomUUID();

    when(mWorkflow.newWorkflowStub(any(), any(WorkflowOptions.class)))
        .thenReturn(mock(ConnectionManagerWorkflow.class));
    when(mWorkflow.newSignalWithStartRequest()).thenReturn(mock(BatchRequest.class));

    final var utils = new ConnectionManagerUtils(mWorkflow, mMetric);
    utils.signalWorkflowAndRepairIfNecessary(cid, (workflow) -> null);
    // Because we do not mock the getConnectionManagerWorkflow call, the underlying call throws an
    // exception
    // and the logic recreates it.
    verify(mWorkflow).signalWithStart(any());
  }

}
