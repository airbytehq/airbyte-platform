/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
import io.airbyte.api.client.model.generated.CheckConnectionRead.StatusEnum;
import io.airbyte.api.client.model.generated.DestinationIdRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import jakarta.inject.Singleton;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * Implementation of SubmitCheckConnectionActivity.
 */
@Slf4j
@Singleton
public class SubmitCheckConnectionActivityImpl implements SubmitCheckConnectionActivity {

  private final SourceApi sourceApi;
  private final DestinationApi destinationApi;
  private final EnvVariableFeatureFlags envVariableFeatureFlags;

  public SubmitCheckConnectionActivityImpl(final SourceApi sourceApi,
                                           final DestinationApi destinationApi,
                                           final EnvVariableFeatureFlags envVariableFeatureFlags) {
    this.sourceApi = sourceApi;
    this.destinationApi = destinationApi;
    this.envVariableFeatureFlags = envVariableFeatureFlags;
  }

  @Override
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  public ConnectorJobOutput submitCheckConnectionToSource(final UUID sourceId) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_SUBMIT_CHECK_SOURCE_CONNECTION, 1);

    ConnectorJobOutput jobOutput = new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION);
    try {
      CheckConnectionRead checkResult = AirbyteApiClient.retryWithJitter(
          () -> sourceApi.checkConnectionToSource(new SourceIdRequestBody().sourceId(sourceId)),
          "Trigger check connection to source");
      jobOutput.withCheckConnection(convertApiOutputToStandardOutput(checkResult));
    } catch (Exception ex) {
      jobOutput.withFailureReason(new FailureReason().withFailureType(FailureType.SYSTEM_ERROR).withInternalMessage(ex.getMessage()));
      throw ex;
    }
    return jobOutput;
  }

  @Override
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  public ConnectorJobOutput submitCheckConnectionToDestination(final UUID destinationId) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_SUBMIT_CHECK_DESTINATION_CONNECTION, 1);

    ConnectorJobOutput jobOutput = new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION);
    try {
      CheckConnectionRead checkResult = AirbyteApiClient.retryWithJitter(
          () -> destinationApi.checkConnectionToDestination(new DestinationIdRequestBody().destinationId(destinationId)),
          "Trigger check connection to destination");
      jobOutput.withCheckConnection(convertApiOutputToStandardOutput(checkResult));
    } catch (Exception ex) {
      jobOutput.withFailureReason(new FailureReason().withFailureType(FailureType.SYSTEM_ERROR).withInternalMessage(ex.getMessage()));
      throw ex;
    }
    return jobOutput;
  }

  private StandardCheckConnectionOutput convertApiOutputToStandardOutput(final CheckConnectionRead apiOutput) {
    StandardCheckConnectionOutput output = new StandardCheckConnectionOutput().withMessage(apiOutput.getMessage());
    if (StatusEnum.SUCCEEDED.equals(apiOutput.getStatus())) {
      output.withStatus(Status.SUCCEEDED);
    } else {
      output.withStatus(Status.FAILED);
    }
    return output;
  }

}
