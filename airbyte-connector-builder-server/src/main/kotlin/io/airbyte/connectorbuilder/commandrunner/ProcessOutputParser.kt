/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import datadog.trace.api.Trace
import io.airbyte.commons.io.IOs
import io.airbyte.connectorbuilder.TracingHelper
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.exceptions.CdkProcessException
import io.airbyte.connectorbuilder.exceptions.CdkUnknownException
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.util.Optional
import java.util.stream.Collectors

/**
 * Extract `AirbyteMessage`s from the process output.
 */
class ProcessOutputParser {
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  @Throws(IOException::class)
  fun parse(
    process: Process,
    streamFactory: AirbyteStreamFactory,
    cdkCommand: String,
  ): AirbyteRecordMessage {
    var messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>? = null
    try {
      messagesByType = WorkerUtils.getMessagesByType(process, streamFactory, TIME_OUT)
    } catch (exc: NullPointerException) {
      throwCdkException(process, cdkCommand)
    } catch (e: IllegalStateException) {
      throw e
    }

    if (messagesByType == null || messagesByType.isEmpty()) {
      throwCdkException(process, cdkCommand)
    }

    val record =
      messagesByType
        ?.getOrDefault(AirbyteMessage.Type.RECORD, ArrayList())
        ?.stream()
        ?.map { obj: AirbyteMessage -> obj.record }
        ?.findFirst() ?: Optional.empty()

    if (record.isPresent) {
      return record.get()
    }

    val trace: Optional<AirbyteTraceMessage> =
      messagesByType
        ?.getOrDefault(AirbyteMessage.Type.TRACE, ArrayList())
        ?.stream()
        ?.map { obj: AirbyteMessage -> obj.trace }
        ?.findFirst() ?: Optional.empty()

    if (trace.isPresent) {
      val traceMessage = trace.get()
      log.debug(
        "Error response from CDK: {}\n{}",
        traceMessage.error.message,
        traceMessage.error.stackTrace,
      )
      throw AirbyteCdkInvalidInputException(
        String.format("AirbyteTraceMessage response from CDK: %s", traceMessage.error.message),
        traceMessage,
      )
    }
    throw generateError(process, cdkCommand)
  }

  private fun throwCdkException(
    process: Process,
    cdkCommand: String,
  ): Unit = throw generateError(process, cdkCommand)

  @Throws(CdkProcessException::class, CdkUnknownException::class)
  private fun generateError(
    process: Process,
    cdkCommand: String,
  ): RuntimeException {
    val exitCode = process.exitValue()
    if (exitCode == 0) {
      val errorMessage =
        String.format(
          "The CDK command `%s` completed properly but no records nor trace were found. Logs were: %s.",
          cdkCommand,
          process.exitValue(),
        )
      log.error(errorMessage)
      return CdkUnknownException(errorMessage)
    }

    val errStream = process.errorStream
    val stderr = IOs.newBufferedReader(errStream)
    val error = stderr.lines().collect(Collectors.joining())

    val errorMessage = String.format("CDK subprocess for %s finished with exit code %d. error=%s", cdkCommand, exitCode, error)
    log.error(errorMessage)
    return CdkProcessException(errorMessage)
  }

  companion object {
    private val log = KotlinLogging.logger {}
    private const val TIME_OUT = 30
  }
}
