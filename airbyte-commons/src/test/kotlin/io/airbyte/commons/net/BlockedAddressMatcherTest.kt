/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.net

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetAddress

class BlockedAddressMatcherTest {
  private val matcher = BlockedAddressMatcher()

  @ParameterizedTest
  @ValueSource(
    strings = [
      "127.0.0.1", "127.255.255.255", "::1", "0.0.0.0", "0.1.2.3", "::",
      "169.254.169.254", "169.254.0.1", "fe80::1", "febf::1", "fd00:ec2::254",
      "10.0.0.1", "10.255.255.255", "172.16.0.1", "172.31.255.255", "192.168.1.1",
      "100.64.0.1", "100.127.255.255", "fc00::1", "fdff::1",
      "224.0.0.1", "239.255.255.255", "ff02::1",
      "::ffff:10.0.0.1", "::ffff:127.0.0.1", "::ffff:169.254.169.254",
    ],
  )
  fun `blocks default ranges`(ip: String) {
    assertTrue(matcher.isBlocked(InetAddress.getByName(ip)), "$ip should be blocked")
    assertTrue(matcher.isBlockedLiteral(ip), "$ip literal should be blocked")
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "8.8.8.8", "1.1.1.1", "93.184.216.34", "172.15.255.255", "172.32.0.1", "100.63.255.255", "100.128.0.1",
      "169.253.1.1", "192.167.1.1", "11.0.0.1", "2606:4700:4700::1111", "2001:4860:4860::8888", "::ffff:8.8.8.8",
    ],
  )
  fun `allows public addresses`(ip: String) {
    assertFalse(matcher.isBlocked(InetAddress.getByName(ip)), "$ip should be allowed")
    assertFalse(matcher.isBlockedLiteral(ip), "$ip literal should be allowed")
  }

  @Test
  fun `bracketed ipv6 literal is handled`() {
    assertTrue(matcher.isBlockedLiteral("[::1]"))
    assertFalse(matcher.isBlockedLiteral("[2606:4700:4700::1111]"))
  }

  @Test
  fun `hostnames are never blocked as literals and never resolved`() {
    assertFalse(matcher.isBlockedLiteral("localhost"))
    assertFalse(matcher.isBlockedLiteral("db.internal.example.com"))
    assertFalse(matcher.isBlockedLiteral(""))
  }

  @Test
  fun `extra cidrs are blocked in addition to defaults`() {
    val extra = BlockedAddressMatcher(listOf("203.0.113.0/24", "2001:db8::/32", "198.51.100.7/32"))

    assertTrue(extra.isBlocked(InetAddress.getByName("203.0.113.55")))
    assertTrue(extra.isBlocked(InetAddress.getByName("2001:db8::1")))
    assertTrue(extra.isBlocked(InetAddress.getByName("198.51.100.7")))
    assertTrue(extra.isBlocked(InetAddress.getByName("10.0.0.1")))
    assertFalse(extra.isBlocked(InetAddress.getByName("203.0.114.1")))
    assertFalse(extra.isBlocked(InetAddress.getByName("198.51.100.8")))
    assertFalse(matcher.isBlocked(InetAddress.getByName("203.0.113.55")))
  }

  @Test
  fun `invalid extra cidrs are rejected`() {
    assertThrows(IllegalArgumentException::class.java) { BlockedAddressMatcher(listOf("not-a-cidr")) }
    assertThrows(IllegalArgumentException::class.java) { BlockedAddressMatcher(listOf("10.0.0.0/33")) }
    assertThrows(IllegalArgumentException::class.java) { BlockedAddressMatcher(listOf("example.com/24")) }
  }
}
