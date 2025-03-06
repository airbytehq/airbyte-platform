/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import datadog.trace.api.Trace;
import io.airbyte.commons.io.IOs;
import io.airbyte.connector_builder.TracingHelper;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.connector_builder.exceptions.CdkUnknownException;
import io.airbyte.connector_builder.requester.AirbyteCdkRequesterImpl;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extract `AirbyteMessage`s from the process output.
 */
public class ProcessOutputParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteCdkRequesterImpl.class);
  private static final int timeOut = 30;

  @SuppressWarnings("PMD.AvoidCatchingNPE")
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  AirbyteRecordMessage parse(
                             final Process process,
                             final AirbyteStreamFactory streamFactory,
                             final String cdkCommand)
      throws IOException {
    Map<Type, List<AirbyteMessage>> messagesByType = null;
    try {
      messagesByType = WorkerUtils.getMessagesByType(process, streamFactory, timeOut);
    } catch (final NullPointerException exc) {
      throwCdkException(process, cdkCommand);
    } catch (final IllegalStateException e) {
      throw e;
    }

    if (messagesByType == null || messagesByType.isEmpty()) {
      throwCdkException(process, cdkCommand);
    }

    final Optional<AirbyteRecordMessage> record = messagesByType
        .getOrDefault(Type.RECORD, new ArrayList<>()).stream()
        .map(AirbyteMessage::getRecord)
        .findFirst();

    if (record.isPresent()) {
      return record.get();
    }

    final Optional<AirbyteTraceMessage> trace = messagesByType
        .getOrDefault(Type.TRACE, new ArrayList<>()).stream()
        .map(AirbyteMessage::getTrace)
        .findFirst();

    if (trace.isPresent()) {
      final AirbyteTraceMessage traceMessage = trace.get();
      LOGGER.debug(
          "Error response from CDK: {}\n{}",
          traceMessage.getError().getMessage(),
          traceMessage.getError().getStackTrace());
      throw new AirbyteCdkInvalidInputException(
          String.format("AirbyteTraceMessage response from CDK: %s", traceMessage.getError().getMessage()), traceMessage);
    }
    throw generateError(process, cdkCommand);
  }

  private void throwCdkException(final Process process, final String cdkCommand) {
    throw generateError(process, cdkCommand);
  }

  private RuntimeException generateError(final Process process, final String cdkCommand)
      throws CdkProcessException, CdkUnknownException {
    final int exitCode = process.exitValue();
    if (exitCode == 0) {
      final String errorMessage = String.format(
          "The CDK command `%s` completed properly but no records nor trace were found. Logs were: %s.", cdkCommand, process.exitValue());
      LOGGER.error(errorMessage);
      return new CdkUnknownException(errorMessage);
    }

    final InputStream errStream = process.getErrorStream();
    final BufferedReader stderr = IOs.newBufferedReader(errStream);
    final String error = stderr.lines().collect(Collectors.joining());

    final String errorMessage = String.format("CDK subprocess for %s finished with exit code %d. error=%s", cdkCommand, exitCode, error);
    LOGGER.error(errorMessage);
    return new CdkProcessException(errorMessage);
  }

}
