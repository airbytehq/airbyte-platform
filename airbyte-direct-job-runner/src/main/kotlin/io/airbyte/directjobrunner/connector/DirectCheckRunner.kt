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
import io.airbyte.directjobrunner.config.connectorName
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Singleton
import org.eclipse.aether.resolution.ArtifactResolutionException
import org.slf4j.MDC
import java.io.IOException
import java.io.InputStream
import java.io.UncheckedIOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.ToDoubleFunction
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

// The JVM options the connector image gives it: JAVA_OPTS from its javabase.sh, and from its launcher script the
// out-of-memory exit (that script's 75%-of-memory heap default is replaced by the explicit heap below).
private val CONNECTOR_JVM_OPTS =
  listOf(
    "-XX:+ExitOnOutOfMemoryError",
    "-Dlog4j.encoder.byteBufferSize=32768",
    "-Dlog4j2.configurationFile=log4j2.xml",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
  )

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

/** Where a check's jar came from. */
enum class JarSource {
  /** The jar was already in [ConnectorJarCache]. */
  CACHED,

  /** The jar wasn't cached; this check waited for it to be downloaded into [ConnectorJarCache]. */
  DOWNLOADED,
}

/** A check's result, where its jar came from, and how long getting the jar and running the connector took. */
data class DirectCheckRun(
  val result: SourceCheckResult,
  val jarSource: JarSource,
  val jarMs: Long,
  val processMs: Long,
)

/**
 * Runs a connector's `check` as a local process: `java -jar` on the connector version's fat jar, from
 * [ConnectorJarCache], downloaded into it first if it isn't cached.
 *
 * Each call starts a new JVM: the connector runs one operation and exits, so nothing is reused between calls.
 */
@Singleton
class DirectCheckRunner(
  private val config: CheckConnectorConfig,
  private val airbyteEdition: Configs.AirbyteEdition,
  private val jarCache: ConnectorJarCache,
  private val metricClient: MetricClient,
) {
  // Downloads run here so a check can stop waiting on one after jarDownloadAllowance.
  private val downloads = Executors.newCachedThreadPool { Thread(it, "check-jar-download").apply { isDaemon = true } }

  init {
    metricClient.gauge(OssMetricsRegistry.DIRECT_CHECK_JAR_CACHE_DISK_BYTES, this, ToDoubleFunction { it.jarDiskBytes() })
  }

  /** How long the connector may run before its process is killed. */
  val checkTimeout: Duration get() = config.checkTimeout

  /** How long a check may spend first downloading a jar that isn't cached. */
  val jarDownloadAllowance: Duration get() = config.jarDownloadAllowance

  /**
   * Whether a check for [image] can run here: its jar is cached or in the jar repository. Nothing is downloaded; a jar
   * that isn't cached yet is downloaded by the check.
   */
  fun canRun(image: String): Boolean = jarCache.isAvailable(config.jarCoordinates(image))

  /**
   * Runs `check` for [image] with [connectionConfiguration], from the jar in [ConnectorJarCache], downloading it into
   * the cache first if it isn't there. A jar that can't be downloaded throws: that is a platform error, not a check
   * result.
   */
  fun check(
    image: String,
    connectionConfiguration: JsonNode,
    workloadId: String = "",
  ): DirectCheckRun {
    val start = System.currentTimeMillis()
    val coordinates = config.jarCoordinates(image)

    val cached = jarCache.get(coordinates)
    metricClient.count(
      OssMetricsRegistry.DIRECT_CHECK_JAR_CACHE_LOOKUP,
      1,
      MetricAttribute(MetricTags.CONNECTOR_IMAGE, image),
      MetricAttribute(MetricTags.RESULT_TAG, if (cached is CachedJar.Hit) "hit" else "miss"),
    )

    return when (cached) {
      is CachedJar.Hit -> run(image, cached.jar, JarSource.CACHED, start, connectionConfiguration, workloadId)
      CachedJar.Miss -> run(image, download(coordinates), JarSource.DOWNLOADED, start, connectionConfiguration, workloadId)
    }
  }

  // The resolver's own timeouts only cover connecting and a stalled transfer, so a slow but steady download is cut off
  // after jarDownloadAllowance, before the workload's deadline. The download itself carries on and caches the jar for
  // later checks; the cache records its result when it ends.
  private fun download(coordinates: String): Path {
    val start = System.currentTimeMillis()
    val download = downloads.submit<Path> { jarCache.getOrDownload(coordinates) }
    try {
      return download.get(config.jarDownloadAllowance.toMillis(), TimeUnit.MILLISECONDS)
    } catch (e: TimeoutException) {
      recordDownloadTimeout(coordinates, start)
      throw IllegalStateException("Downloading $coordinates for the check took longer than ${config.jarDownloadAllowance}", e)
    } catch (e: ExecutionException) {
      val cause = e.cause
      if (cause is ArtifactResolutionException) {
        throw IllegalStateException("Couldn't download $coordinates for the check", cause)
      }
      throw e
    }
  }

  private fun recordDownloadTimeout(
    coordinates: String,
    start: Long,
  ) {
    val tags =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTOR_IMAGE, connectorImageTag(coordinates)),
        MetricAttribute(MetricTags.RESULT_TAG, RESULT_TIMEOUT),
      )
    metricClient.count(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD, 1, *tags)
    metricClient.distribution(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD_DURATION, (System.currentTimeMillis() - start).toDouble(), *tags)
  }

  // Everything under the jar cache directory: cached jars and evicted jars not yet deleted. A file deleted during the
  // walk (an eviction) skips this sample rather than reporting a wrong total.
  private fun jarDiskBytes(): Double {
    val root = Path.of(config.jarCacheDir)
    if (!Files.exists(root)) {
      return 0.0
    }
    return try {
      Files.walk(root).use { paths ->
        paths
          .filter { Files.isRegularFile(it) }
          .mapToLong { Files.size(it) }
          .sum()
          .toDouble()
      }
    } catch (e: IOException) {
      Double.NaN
    } catch (e: UncheckedIOException) {
      Double.NaN
    }
  }

  private fun run(
    image: String,
    jar: Path,
    jarSource: JarSource,
    start: Long,
    connectionConfiguration: JsonNode,
    workloadId: String,
  ): DirectCheckRun {
    val jarAt = System.currentTimeMillis()
    val result = runConnector(connectorName(image), jar, connectionConfiguration, workloadId)
    return DirectCheckRun(result, jarSource, jarMs = jarAt - start, processMs = System.currentTimeMillis() - jarAt)
  }

  // The connector reads `--config` as a path, so it gets /dev/stdin.
  private fun runConnector(
    name: String,
    jar: Path,
    connectionConfiguration: JsonNode,
    workloadId: String,
  ): SourceCheckResult {
    // An explicit heap, rather than the image launcher's 75% of container memory. Several connector JVMs share a node, so
    // each also sizes its GC and JIT threads for activeProcessorCount CPUs, not all of them.
    val command =
      listOf(config.java.toString(), "-Xms64m", "-Xmx${config.maxHeap}", "-XX:ActiveProcessorCount=${config.activeProcessorCount}") +
        CONNECTOR_JVM_OPTS +
        listOf("-jar", jar.toString(), "--check", "--config", "/dev/stdin")
    val builder = ProcessBuilder(command)
    builder.environment().apply {
      clear()
      putAll(System.getenv().filterKeys { it in INHERITED_ENV })
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
      thread(name = "$name-stdout-${process.pid()}", isDaemon = true) {
        withLoggingContext(connectorLogContext) { readMessages(name, process.inputStream, messages) }
      }
    val stderr =
      thread(name = "$name-stderr-${process.pid()}", isDaemon = true) {
        process.errorStream.bufferedReader().forEachLine { logger.info { "$name stderr: $it" } }
      }
    // The config holds credentials, so it goes over stdin and never touches disk. Written on its own thread so the
    // timeout below also covers a connector that never reads it.
    thread(name = "$name-stdin-${process.pid()}", isDaemon = true) {
      try {
        process.outputStream.use { it.write(Jsons.serialize(connectionConfiguration).toByteArray()) }
      } catch (e: IOException) {
        // A connector that exits before reading closes the pipe; its output still decides the result.
        logger.info { "$name closed stdin before reading the config: ${e.message}" }
      }
    }

    val finished = process.waitFor(config.checkTimeout.toMillis(), TimeUnit.MILLISECONDS)
    if (!finished) {
      // Kill anything the connector started too, not just its JVM.
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
    name: String,
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
              logger.info { "$name: ${message.log.message}" }
            } else {
              messages.add(message)
            }
          },
          { logger.info { "$name: $line" } },
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
