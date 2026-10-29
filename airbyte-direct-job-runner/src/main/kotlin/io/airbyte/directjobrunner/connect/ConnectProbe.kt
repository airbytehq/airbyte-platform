/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connect

import io.airbyte.commons.net.BlockedAddressMatcher
import io.airbyte.config.ConnectErrorCode
import io.airbyte.config.ConnectInput
import io.airbyte.config.ConnectOutput
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
 */
class ConnectProbe(
  private val matcher: BlockedAddressMatcher,
  private val overallTimeout: Duration,
  private val resolver: HostResolver = SystemHostResolver,
  private val connector: TcpConnector = SocketTcpConnector,
) {
  fun probe(input: ConnectInput): ConnectOutput {
    val startNanos = System.nanoTime()

    fun elapsedMs(): Long = Duration.ofNanos(System.nanoTime() - startNanos).toMillis()

    val addresses =
      try {
        resolver.resolve(input.host)
      } catch (e: UnknownHostException) {
        return ConnectOutput(
          reachable = false,
          errorCode = ConnectErrorCode.DNS_FAILED,
          message = "Could not resolve host ${input.host}",
          connectLatencyMs = elapsedMs(),
        )
      }

    // Blocked destinations are reported as refused, without attempting a connection or returning the resolved addresses.
    val blocked = addresses.filter { matcher.isBlocked(it) }
    if (blocked.isNotEmpty()) {
      return ConnectOutput(
        reachable = false,
        errorCode = ConnectErrorCode.CONNECTION_REFUSED,
        message = "Host ${input.host} resolves to an address that is not allowed to be reached (${blocked.first().hostAddress})",
        connectLatencyMs = elapsedMs(),
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
      try {
        connector.connect(target, timeoutMs)
        return ConnectOutput(
          reachable = true,
          errorCode = ConnectErrorCode.NONE,
          message = "Connected to ${address.hostAddress}:${input.port}",
          resolvedAddresses = resolvedAddresses,
          connectLatencyMs = elapsedMs(),
        )
      } catch (e: ConnectException) {
        failures += Failure(ConnectErrorCode.CONNECTION_REFUSED, "Connection refused by ${address.hostAddress}:${input.port}")
      } catch (e: SocketTimeoutException) {
        failures += Failure(ConnectErrorCode.TIMEOUT, "Timed out connecting to ${address.hostAddress}:${input.port} after ${timeoutMs}ms")
      } catch (e: IOException) {
        failures += Failure(ConnectErrorCode.UNKNOWN, "Could not connect to ${address.hostAddress}:${input.port}: ${e.message}")
      }
    }

    val failure =
      failures.minByOrNull { it.priority }
        ?: Failure(ConnectErrorCode.TIMEOUT, "Timed out connecting to ${input.host}:${input.port} after ${overallTimeout.toMillis()}ms")
    return ConnectOutput(
      reachable = false,
      errorCode = failure.code,
      message = failure.message,
      resolvedAddresses = resolvedAddresses,
      connectLatencyMs = elapsedMs(),
    )
  }

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
