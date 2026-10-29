/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.net

import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress

/**
 * Decides whether an address belongs to a range that user-supplied hosts must never be probed or connected to
 * (loopback, private, link-local / cloud metadata, carrier-grade NAT, multicast, unspecified).
 *
 * @param extraBlockedCidrs additional CIDR ranges (IPv4 or IPv6, e.g. "203.0.113.0/24") blocked on top of the defaults.
 * @param defaultBlockedCidrs the built-in ranges; only override this in tests that need to reach loopback.
 */
class BlockedAddressMatcher(
  extraBlockedCidrs: List<String> = emptyList(),
  defaultBlockedCidrs: List<String> = DEFAULT_BLOCKED_CIDRS,
) {
  private val blockedRanges: List<CidrRange> = (defaultBlockedCidrs + extraBlockedCidrs).map(CidrRange::parse)

  fun isBlocked(address: InetAddress): Boolean {
    val bytes = normalize(address)
    return blockedRanges.any { it.contains(bytes) }
  }

  /**
   * True only when [host] is a valid IP literal inside a blocked range. Never performs a DNS lookup: hostnames and
   * malformed literals return false.
   */
  fun isBlockedLiteral(host: String): Boolean = parseIpLiteral(host)?.let(::isBlocked) ?: false

  // IPv4-mapped IPv6 addresses are already unwrapped to Inet4Address by the JDK when parsed from bytes, but be explicit.
  private fun normalize(address: InetAddress): ByteArray {
    val bytes = address.address
    if (address is Inet6Address && isIpv4Mapped(bytes)) {
      return bytes.copyOfRange(IPV4_MAPPED_OFFSET, IPV6_LENGTH)
    }
    return bytes
  }

  private fun isIpv4Mapped(bytes: ByteArray): Boolean =
    bytes.size == IPV6_LENGTH &&
      (0 until IPV4_MAPPED_PREFIX_ZEROES).all { bytes[it] == 0.toByte() } &&
      bytes[10] == 0xff.toByte() &&
      bytes[11] == 0xff.toByte()

  private class CidrRange(
    private val network: ByteArray,
    private val prefixLength: Int,
  ) {
    fun contains(address: ByteArray): Boolean {
      if (address.size != network.size) {
        return false
      }
      val fullBytes = prefixLength / BITS_PER_BYTE
      for (i in 0 until fullBytes) {
        if (address[i] != network[i]) return false
      }
      val remainingBits = prefixLength % BITS_PER_BYTE
      if (remainingBits == 0) {
        return true
      }
      val mask = (0xff shl (BITS_PER_BYTE - remainingBits)) and 0xff
      return (address[fullBytes].toInt() and mask) == (network[fullBytes].toInt() and mask)
    }

    companion object {
      fun parse(cidr: String): CidrRange {
        val parts = cidr.trim().split("/")
        require(parts.size == 2) { "Invalid CIDR '$cidr': expected <address>/<prefix-length>" }
        val address = requireNotNull(parseIpLiteral(parts[0].trim())) { "Invalid CIDR '$cidr': address must be an IP literal" }
        val prefix = requireNotNull(parts[1].trim().toIntOrNull()) { "Invalid CIDR '$cidr': prefix length is not a number" }
        val maxPrefix = if (address is Inet4Address) IPV4_LENGTH * BITS_PER_BYTE else IPV6_LENGTH * BITS_PER_BYTE
        require(prefix in 0..maxPrefix) { "Invalid CIDR '$cidr': prefix length must be between 0 and $maxPrefix" }
        return CidrRange(address.address, prefix)
      }
    }
  }

  companion object {
    val DEFAULT_BLOCKED_CIDRS: List<String> =
      listOf(
        // loopback
        "127.0.0.0/8",
        "::1/128",
        // "this network" and unspecified
        "0.0.0.0/8",
        "::/128",
        // link-local, including the cloud metadata endpoints
        "169.254.0.0/16",
        "fe80::/10",
        "fd00:ec2::254/128",
        // RFC1918 private ranges
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        // carrier-grade NAT
        "100.64.0.0/10",
        // IPv6 unique local
        "fc00::/7",
        // multicast
        "224.0.0.0/4",
        "ff00::/8",
      )

    private const val BITS_PER_BYTE = 8
    private const val IPV4_LENGTH = 4
    private const val IPV6_LENGTH = 16
    private const val IPV4_MAPPED_OFFSET = 12
    private const val IPV4_MAPPED_PREFIX_ZEROES = 10
  }
}
