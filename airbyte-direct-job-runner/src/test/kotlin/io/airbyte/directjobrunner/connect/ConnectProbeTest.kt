/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connect

import io.airbyte.commons.net.BlockedAddressMatcher
import io.airbyte.config.ConnectErrorCode
import io.airbyte.config.ConnectInput
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NoRouteToHostException
import java.net.ServerSocket
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.time.Duration
import java.util.UUID

class ConnectProbeTest {
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()

  private fun input(
    host: String = "db.example.com",
    port: Int = 5432,
    connectTimeoutMs: Long = 5000,
  ) = ConnectInput(host, port, workspaceId, organizationId, connectTimeoutMs)

  private fun ip(value: String) = InetAddress.getByName(value)

  private fun probe(
    resolver: HostResolver,
    connector: TcpConnector = TcpConnector { _, _ -> },
    matcher: BlockedAddressMatcher = BlockedAddressMatcher(),
    overallTimeout: Duration = Duration.ofSeconds(10),
  ) = ConnectProbe(matcher, overallTimeout, resolver, connector)

  @Test
  fun `connects to an open port`() {
    ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")).use { server ->
      // loopback is blocked by default, so use a matcher without the default ranges against the real resolver and socket
      val allowLoopback = ConnectProbe(BlockedAddressMatcher(defaultBlockedCidrs = emptyList()), Duration.ofSeconds(10))

      val output = allowLoopback.probe(input(host = "127.0.0.1", port = server.localPort))

      assertTrue(output.reachable)
      assertEquals(ConnectErrorCode.NONE, output.errorCode)
      assertEquals(listOf("127.0.0.1"), output.resolvedAddresses)
    }
  }

  @Test
  fun `reports connection refused for a closed port`() {
    val closedPort = ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")).use { it.localPort }
    val allowLoopback = ConnectProbe(BlockedAddressMatcher(defaultBlockedCidrs = emptyList()), Duration.ofSeconds(10))

    val output = allowLoopback.probe(input(host = "127.0.0.1", port = closedPort))

    assertFalse(output.reachable)
    assertEquals(ConnectErrorCode.CONNECTION_REFUSED, output.errorCode)
    assertTrue(output.message.contains("Connection refused by 127.0.0.1:$closedPort"), output.message)
  }

  @Test
  fun `reports timeout`() {
    val output =
      probe(
        resolver = { listOf(ip("8.8.8.8")) },
        connector = { _, _ -> throw SocketTimeoutException("connect timed out") },
      ).probe(input(connectTimeoutMs = 50))

    assertFalse(output.reachable)
    assertEquals(ConnectErrorCode.TIMEOUT, output.errorCode)
    assertTrue(output.message.contains("8.8.8.8:5432"), output.message)
  }

  @Test
  fun `reports unknown for other io errors`() {
    val output =
      probe(
        resolver = { listOf(ip("8.8.8.8")) },
        connector = { _, _ -> throw NoRouteToHostException("No route to host") },
      ).probe(input())

    assertEquals(ConnectErrorCode.UNKNOWN, output.errorCode)
    assertTrue(output.message.contains("No route to host"), output.message)
  }

  @Test
  fun `reports dns failure`() {
    val output = probe(resolver = { throw UnknownHostException("nope") }).probe(input(host = "nope.example.com"))

    assertFalse(output.reachable)
    assertEquals(ConnectErrorCode.DNS_FAILED, output.errorCode)
    assertTrue(output.resolvedAddresses.isEmpty())
    assertTrue(output.message.contains("nope.example.com"), output.message)
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "127.0.0.1", "::1", "0.0.0.0", "169.254.169.254", "fe80::1", "fd00:ec2::254", "10.1.2.3",
      "172.16.5.5", "192.168.0.10", "100.64.1.1", "fc00::5", "224.0.0.5", "ff02::1", "::ffff:10.0.0.1", "::",
    ],
  )
  fun `blocks every denied range without connecting`(address: String) {
    var connected = false
    val output =
      probe(
        resolver = { listOf(ip(address)) },
        connector = { _, _ -> connected = true },
      ).probe(input())

    assertFalse(connected, "must not connect to $address")
    assertFalse(output.reachable)
    assertEquals(ConnectErrorCode.CONNECTION_REFUSED, output.errorCode)
    assertTrue(output.message.contains("not allowed"))
    assertTrue(output.resolvedAddresses.isEmpty())
  }

  @Test
  fun `blocks when any resolved address is private even if others are public`() {
    var connected = false
    val output =
      probe(
        resolver = { listOf(ip("8.8.8.8"), ip("10.0.0.5")) },
        connector = { _, _ -> connected = true },
      ).probe(input())

    assertFalse(connected)
    assertEquals(ConnectErrorCode.CONNECTION_REFUSED, output.errorCode)
    assertTrue(output.message.contains("not allowed"))
    assertTrue(output.resolvedAddresses.isEmpty())
  }

  @Test
  fun `blocks configured extra cidrs`() {
    val output =
      probe(
        resolver = { listOf(ip("203.0.113.9")) },
        matcher = BlockedAddressMatcher(listOf("203.0.113.0/24")),
      ).probe(input())

    assertEquals(ConnectErrorCode.CONNECTION_REFUSED, output.errorCode)
    assertTrue(output.message.contains("not allowed"))
  }

  @Test
  fun `falls through to the next address and succeeds`() {
    val attempted = mutableListOf<String>()
    val output =
      probe(
        resolver = { listOf(ip("2606:4700:4700::1111"), ip("1.1.1.1")) },
        connector = { address, _ ->
          attempted += address.address.hostAddress
          if (address.address is java.net.Inet6Address) throw NoRouteToHostException("no v6")
        },
      ).probe(input())

    assertTrue(output.reachable)
    assertEquals(listOf("2606:4700:4700:0:0:0:0:1111", "1.1.1.1"), attempted)
    assertEquals(ConnectErrorCode.NONE, output.errorCode)
  }

  @Test
  fun `prefers refusal over other failures when all addresses fail`() {
    val output =
      probe(
        resolver = { listOf(ip("2606:4700:4700::1111"), ip("1.1.1.1")) },
        connector = { address, _ ->
          if (address.address is java.net.Inet6Address) throw NoRouteToHostException("no v6") else throw ConnectException("refused")
        },
      ).probe(input())

    assertEquals(ConnectErrorCode.CONNECTION_REFUSED, output.errorCode)
  }

  @Test
  fun `caps each attempt by the input timeout and the overall budget`() {
    val timeouts = mutableListOf<Int>()
    probe(
      resolver = { listOf(ip("1.1.1.1")) },
      connector = { _, timeoutMs -> timeouts += timeoutMs },
      overallTimeout = Duration.ofMillis(200),
    ).probe(input(connectTimeoutMs = 5000))

    assertEquals(1, timeouts.size)
    assertTrue(timeouts.single() in 1..200, "timeout was ${timeouts.single()}")
  }

  @Test
  fun `stops trying addresses once the overall budget is spent`() {
    val attempted = mutableListOf<InetSocketAddress>()
    val output =
      probe(
        resolver = { listOf(ip("1.1.1.1"), ip("1.0.0.1"), ip("8.8.4.4")) },
        connector = { address, _ ->
          attempted += address
          Thread.sleep(60)
          throw SocketTimeoutException("timed out")
        },
        overallTimeout = Duration.ofMillis(100),
      ).probe(input())

    assertTrue(attempted.size < 3, "attempted ${attempted.size}")
    assertEquals(ConnectErrorCode.TIMEOUT, output.errorCode)
  }
}
