/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogSource
import io.airbyte.config.Configs
import io.airbyte.config.WorkloadType
import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Singleton
import org.slf4j.MDC
import java.io.IOException
import java.io.InputStream
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

// Mirrors the JAVA_OPTS the connector image's javabase.sh sets; we call the launcher directly and skip that script.
private const val CONNECTOR_JAVA_OPTS =
  "-Dlog4j.encoder.byteBufferSize=32768 -Dlog4j2.configurationFile=log4j2.xml --add-opens=java.base/java.lang=ALL-UNNAMED"

// The connector gets only these from the runner's environment, never its credentials.
private val INHERITED_ENV = setOf("PATH", "JAVA_HOME", "LANG")

// The pipes reach EOF once every holder of their write end exits; bound the wait in case a grandchild kept one open.
private val READER_DRAIN_TIMEOUT = Duration.ofSeconds(5)

sealed interface SourceCheckResult {
  /** The connector reported a status. FAILED is a normal outcome (bad credentials, unreachable host). */
  data class Reported(
    val status: AirbyteConnectionStatus,
  ) : SourceCheckResult

  /** The connector emitted an error trace instead of a status. */
  data class ConnectorError(
    val error: AirbyteErrorTraceMessage,
  ) : SourceCheckResult

  /** The connector exited without a status or an error trace. */
  data class Crashed(
    val exitCode: Int,
  ) : SourceCheckResult

  data object TimedOut : SourceCheckResult
}

/**
 * Runs a connector's `check` as a local process. Today the only implementation is the source-postgres distribution
 * baked into this image; other connectors (or a jar cache) can come in behind the same calls.
 *
 * Each call starts a new JVM: the connector runs one operation and exits, so nothing is reused between calls.
 */
@Singleton
class DirectCheckRunner(
  private val config: CheckConnectorConfig,
  private val airbyteEdition: Configs.AirbyteEdition,
) {
  /** The connector image this check runs, such as `airbyte/source-postgres:3.8.10`. */
  val image: String get() = config.image

  /** How long one check may run before its process is killed. */
  val checkTimeout: Duration get() = config.checkTimeout

  // The connector reads `--config` as a path, so it gets /dev/stdin.
  fun check(
    connectionConfiguration: JsonNode,
    workloadId: String = "",
  ): SourceCheckResult {
    val builder = ProcessBuilder(config.launcher.toString(), "--check", "--config", "/dev/stdin")
    builder.environment().apply {
      clear()
      putAll(System.getenv().filterKeys { it in INHERITED_ENV })
      put("JAVA_OPTS", CONNECTOR_JAVA_OPTS)
      // The launcher defaults to 75% of container memory for both initial and max heap; cap both.
      put(config.jvmOptsEnvVar, "-Xms64m -Xmx${config.maxHeap}")
      // What a check pod's connector gets (RuntimeEnvVarFactory.checkConnectorEnvVars and the launcher's
      // airbyteMetadataEnvMap). The edition changes the result: on Cloud the connector rejects unencrypted
      // connections, so without it the runner would pass checks a sync then fails. Bulk CDK connectors such as
      // source-postgres read AIRBYTE_EDITION; older Java CDK connectors read DEPLOYMENT_MODE.
      put("AIRBYTE_EDITION", airbyteEdition.name)
      put("DEPLOYMENT_MODE", if (airbyteEdition == Configs.AirbyteEdition.CLOUD) "CLOUD" else "OSS")
      // TODO: take this from the workload input instead of hardcoding CHECK. Staged checks will need to say which stage
      // to run, and that will come through this field.
      put("OPERATION_TYPE", WorkloadType.CHECK.toString())
      put("WORKLOAD_ID", workloadId)
    }
    val process = builder.start()

    // Drain both pipes before writing the config: a connector that logs a lot before reading stdin would otherwise
    // block on a full pipe while this thread blocks writing to it. The stdout thread doesn't inherit the caller's
    // logging context (the job's log path), so it gets a copy, with the connector's lines labeled as source logs.
    // stderr goes only to the runner's own log, like a check pod's container stderr. Daemon threads, so one still
    // blocked on a pipe held open by a leftover child can't keep the runner from shutting down.
    val connectorLogContext = (MDC.getCopyOfContextMap() ?: emptyMap()) + LogSource.SOURCE.toMdc()
    val messages = ConcurrentLinkedQueue<AirbyteMessage>()
    val stdout =
      thread(name = "${config.name}-stdout-${process.pid()}", isDaemon = true) {
        withLoggingContext(connectorLogContext) { readMessages(process.inputStream, messages) }
      }
    val stderr =
      thread(name = "${config.name}-stderr-${process.pid()}", isDaemon = true) {
        process.errorStream.bufferedReader().forEachLine { logger.info { "${config.name} stderr: $it" } }
      }
    // The config holds credentials, so it goes over stdin and never touches disk. Written on its own thread so the
    // timeout below also covers a connector that never reads it.
    thread(name = "${config.name}-stdin-${process.pid()}", isDaemon = true) {
      try {
        process.outputStream.use { it.write(Jsons.serialize(connectionConfiguration).toByteArray()) }
      } catch (e: IOException) {
        // A connector that exits before reading closes the pipe; its output still decides the result.
        logger.info { "${config.name} closed stdin before reading the config: ${e.message}" }
      }
    }

    val finished = process.waitFor(config.checkTimeout.toMillis(), TimeUnit.MILLISECONDS)
    if (!finished) {
      // The launcher script may have started the JVM as a child, so kill the whole tree, not just the script.
      process.descendants().forEach { it.destroyForcibly() }
      process.destroyForcibly().waitFor()
    }
    stdout.join(READER_DRAIN_TIMEOUT.toMillis())
    stderr.join(READER_DRAIN_TIMEOUT.toMillis())
    // Close the pipes so a reader still blocked on one held open by a leftover child returns.
    runCatching { process.inputStream.close() }
    runCatching { process.errorStream.close() }

    if (!finished) {
      return SourceCheckResult.TimedOut
    }
    return toResult(messages, process.exitValue())
  }

  // A line that is not an Airbyte message is logged rather than thrown on: a dead reader stops draining the pipe,
  // which blocks the connector until the timeout and hides the real outcome.
  private fun readMessages(
    output: InputStream,
    messages: MutableCollection<AirbyteMessage>,
  ) {
    output.bufferedReader().forEachLine { line ->
      Jsons
        .tryDeserialize(line)
        .filter { it.has("type") }
        .flatMap { Jsons.tryObject(it, AirbyteMessage::class.java) }
        .ifPresentOrElse(
          { message ->
            if (message.type == AirbyteMessage.Type.LOG) {
              logger.info { "${config.name}: ${message.log.message}" }
            } else {
              messages.add(message)
            }
          },
          { logger.info { "${config.name}: $line" } },
        )
    }
  }

  // AirbyteMessage is a generated Java model; its nullable fields are converted to the sealed result here.
  private fun toResult(
    messages: Collection<AirbyteMessage>,
    exitCode: Int,
  ): SourceCheckResult {
    val status = messages.firstOrNull { it.type == AirbyteMessage.Type.CONNECTION_STATUS }?.connectionStatus
    if (status != null) {
      return SourceCheckResult.Reported(status)
    }

    val error =
      messages
        .filter { it.type == AirbyteMessage.Type.TRACE && it.trace.type == AirbyteTraceMessage.Type.ERROR }
        .map { it.trace.error }
        .firstOrNull()
    if (error != null) {
      return SourceCheckResult.ConnectorError(error)
    }

    return SourceCheckResult.Crashed(exitCode)
  }
}
