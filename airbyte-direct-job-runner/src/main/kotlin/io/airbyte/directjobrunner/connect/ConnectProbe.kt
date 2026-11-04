/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connect

import io.airbyte.commons.net.BlockedAddressMatcher
import io.airbyte.config.ConnectErrorCode
import io.airbyte.config.ConnectInput
import io.airbyte.config.ConnectOutput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import java.io.IOException
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.time.Duration

fun interface HostResolver {
  /** Returns every address the host resolves to, or throws [UnknownHostException]. */
  fun resolve(host: String): List<InetAddress>
}

fun interface TcpConnector {
  /** Opens and immediately closes a TCP connection, throwing the underlying [IOException] on failure. */
  fun connect(
    address: InetSocketAddress,
    timeoutMs: Int,
  )
}

object SystemHostResolver : HostResolver {
  override fun resolve(host: String): List<InetAddress> = InetAddress.getAllByName(host).distinct()
}

object SocketTcpConnector : TcpConnector {
  override fun connect(
    address: InetSocketAddress,
    timeoutMs: Int,
  ) {
    Socket().use { it.connect(address, timeoutMs) }
  }
}

/**
 * Checks that a TCP connection to a user-supplied host and port can be opened from this data plane.
 *
 * Every resolved address is checked against [matcher] before any connection is attempted, and connections are made
 * to the addresses that were checked (the host is never resolved a second time), so DNS cannot be used to reach
 * internal ranges.
 *
 * Metrics never carry the host, port, or resolved addresses: they are user-supplied and unbounded.
 */
class ConnectProbe(
  private val matcher: BlockedAddressMatcher,
  private val overallTimeout: Duration,
  private val metricClient: MetricClient,
  private val resolver: HostResolver = SystemHostResolver,
  private val connector: TcpConnector = SocketTcpConnector,
) {
  fun probe(input: ConnectInput): ConnectOutput {
    val result = runProbe(input)
    metricClient.count(
      OssMetricsRegistry.CONNECT_PROBE,
      1,
      MetricAttribute(MetricTags.OUTCOME, result.outcome),
      MetricAttribute(MetricTags.TIMEOUT_BUDGET_EXHAUSTED_TAG, result.timeoutBudgetExhausted.toString()),
    )
    return result.output
  }

  private fun runProbe(input: ConnectInput): ProbeResult {
    val startNanos = System.nanoTime()

    fun elapsedMs(): Long = Duration.ofNanos(System.nanoTime() - startNanos).toMillis()

    val addresses =
      try {
        resolver.resolve(input.host).also { recordDuration(OssMetricsRegistry.CONNECT_PROBE_DNS_DURATION, startNanos, "success") }
      } catch (e: UnknownHostException) {
        recordDuration(OssMetricsRegistry.CONNECT_PROBE_DNS_DURATION, startNanos, "failure")
        return ProbeResult(
          ConnectOutput(
            reachable = false,
            errorCode = ConnectErrorCode.DNS_FAILED,
            message = "Could not resolve host ${input.host}",
            connectLatencyMs = elapsedMs(),
          ),
        )
      }

    // Blocked destinations are reported as refused, without attempting a connection or returning the resolved addresses.
    // Only the metric tells them apart from a real refusal.
    val blocked = addresses.filter { matcher.isBlocked(it) }
    if (blocked.isNotEmpty()) {
      return ProbeResult(
        ConnectOutput(
          reachable = false,
          errorCode = ConnectErrorCode.CONNECTION_REFUSED,
          message = "Host ${input.host} resolves to an address that is not allowed to be reached (${blocked.first().hostAddress})",
          connectLatencyMs = elapsedMs(),
        ),
        outcome = OUTCOME_BLOCKED,
      )
    }

    val resolvedAddresses = addresses.map { it.hostAddress }

    val failures = mutableListOf<Failure>()
    for (address in addresses) {
      val remainingMs = overallTimeout.toMillis() - elapsedMs()
      if (remainingMs <= 0) {
        break
      }
      val timeoutMs = minOf(input.connectTimeoutMs, remainingMs).toInt().coerceAtLeast(1)
      val target = InetSocketAddress(address, input.port)
      val attemptStartNanos = System.nanoTime()
      try {
        connector.connect(target, timeoutMs)
        recordDuration(OssMetricsRegistry.CONNECT_PROBE_TCP_CONNECT_DURATION, attemptStartNanos, "connected")
        return ProbeResult(
          ConnectOutput(
            reachable = true,
            errorCode = ConnectErrorCode.NONE,
            message = "Connected to ${address.hostAddress}:${input.port}",
            resolvedAddresses = resolvedAddresses,
            connectLatencyMs = elapsedMs(),
          ),
        )
      } catch (e: ConnectException) {
        failures += Failure(ConnectErrorCode.CONNECTION_REFUSED, "Connection refused by ${address.hostAddress}:${input.port}")
      } catch (e: SocketTimeoutException) {
        failures += Failure(ConnectErrorCode.TIMEOUT, "Timed out connecting to ${address.hostAddress}:${input.port} after ${timeoutMs}ms")
      } catch (e: IOException) {
        failures += Failure(ConnectErrorCode.UNKNOWN, "Could not connect to ${address.hostAddress}:${input.port}: ${e.message}")
      }
      recordDuration(OssMetricsRegistry.CONNECT_PROBE_TCP_CONNECT_DURATION, attemptStartNanos, failures.last().code.metricValue())
    }

    val failure =
      failures.minByOrNull { it.priority }
        ?: Failure(ConnectErrorCode.TIMEOUT, "Timed out connecting to ${input.host}:${input.port} after ${overallTimeout.toMillis()}ms")
    val connectLatencyMs = elapsedMs()
    return ProbeResult(
      ConnectOutput(
        reachable = false,
        errorCode = failure.code,
        message = failure.message,
        resolvedAddresses = resolvedAddresses,
        connectLatencyMs = connectLatencyMs,
      ),
      timeoutBudgetExhausted = connectLatencyMs >= overallTimeout.toMillis(),
    )
  }

  private fun recordDuration(
    metric: OssMetricsRegistry,
    startNanos: Long,
    result: String,
  ) {
    val elapsedMs = Duration.ofNanos(System.nanoTime() - startNanos).toMillis().toDouble()
    metricClient.distribution(metric, elapsedMs, MetricAttribute(MetricTags.RESULT_TAG, result))
  }

  /** [outcome] defaults to the error code; only blocked destinations differ, since they report a refusal. */
  private data class ProbeResult(
    val output: ConnectOutput,
    val outcome: String = if (output.reachable) OUTCOME_REACHABLE else output.errorCode.metricValue(),
    val timeoutBudgetExhausted: Boolean = false,
  )

  // When several addresses fail differently, report the most specific cause: a refusal proves the host was reached.
  private data class Failure(
    val code: ConnectErrorCode,
    val message: String,
  ) {
    val priority: Int =
      when (code) {
        ConnectErrorCode.CONNECTION_REFUSED -> 0
        ConnectErrorCode.TIMEOUT -> 1
        else -> 2
      }
  }
}

private const val OUTCOME_REACHABLE = "reachable"
private const val OUTCOME_BLOCKED = "blocked"

private fun ConnectErrorCode.metricValue(): String = name.lowercase()
